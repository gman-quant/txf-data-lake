# -*- coding: utf-8 -*-
"""TXO 早報(05:05)—— 夜盤實況計分 + 明日日盤重新錨定,寫回**同一份**報表。

用法:
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py                     # 排程(Kafka)
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py --date 2026-08-31 --source lake  # 歷史重放
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py --mode open         # 08:46 第三層
  加 --dry-run 只算不寫。

設計(2026-09-01,使用者拍板「檔案視同一份、內容新增」):
  ‧ 14:25 的預測由 write_forecast() 凍結在 forecast/fc_D.json —— 本腳本**只讀不重算**,
    「昨天預測了什麼」不受日後改碼影響,計分板才有意義。
  ‧ HTML 更新走 marker 置換(冪等):MORNING-BEGIN..END 整段換掉,重跑不會疊兩份;
    gex_D.html 的第 1 層(14:25 產)一個位元組都不動。
  ‧ 依據(session 量測,詳 CALIB 註解):
      - 夜盤只搬錨點不帶方向(r=+0.058);錨改夜盤收盤 → 帶寬合計 5.04σ→3.15σ。
      - 夜盤振幅預告日盤振幅 → σ′ = σ·√(1−W+W·(nr/NR_MED)²),W=0.6(MMSE 0.581 與
        帶寬掃描 0.6~0.7 兩條獨立路收斂)。躁動夜盤後,舊帶實蓋 50% → σ′ 修回 64~69%。
      - 開盤價是開盤前一切的充分統計量(跳空 vs 開盤後走勢 R²=0.72%)→ 開盤換算卡;
        連假後夜盤錨失準(蓋 37%)而開盤錨不受影響(70%)。
  ‧ 失效哲學:**絕不用陳舊資料硬畫一份看起來正常的早報**。
      夜盤 5m ≥150/168 → RV 路徑(W=0.6)
      棒數不足但有高低點 → Parkinson 降級(W=0.4),報表標「降級」
      整夜 0 筆(前一日為假日 → 無夜盤)→ no-op,不算失敗
      broker 連不上 / offsets 出界 / fc 缺 → 失敗,寫 state,不碰 HTML
  ‧ 健康:logs/morning_state.json(連續失敗計數);14:25 的 txo_gex_daily 會讀它,
    連續失敗 ≥2 就在自己的 log 吵。不可變稽核 = logs/morning_history.jsonl
    (每次執行快照「當時讀到的 fc + 夜盤實況 + 算出的帶」)。

⚠ 時戳陷阱(兩個,別搞混):
  ① 本機時區=台北 ⇒ naive datetime 直接 .timestamp(),**不可再手動 −8h**
    (2026-08-31 犯過:減兩次 seek 到日盤開盤)。
  ② pb.timestamp_ms 與 broker ts 都是正規 UTC epoch ms;轉台北 = fromtimestamp(utc)+8h。
    (Shioaji api.ticks 的「台北當 UTC」陷阱是**另一回事**,不在 Kafka 這條路上。)
"""
import sys
import io
import json
import math
import uuid
import argparse
import importlib.util
from datetime import date, datetime, timedelta
from pathlib import Path

# 入口自保:不管誰怎麼啟動都不因 cp950 崩(workspace 三層防線的第一層)
for _s in ("stdout", "stderr"):
    _st = getattr(sys, _s)
    if hasattr(_st, "reconfigure"):
        _st.reconfigure(encoding="utf-8", errors="replace")

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import polars as pl                                            # noqa: E402
from txo_gex_daily import (CALIB, TXO_ROOT, _svg_lanes, _card_html,  # noqa: E402
                           _lanes_from)
from config.lake_paths import kbar_paths                       # noqa: E402

PLATFORM = _HERE.parent / "txf-quant-platform"
KAFKA_BROKER = "192.168.1.50:9092"          # 與 platform config 同值;搬 broker 要一起改
FC_DIR = TXO_ROOT / "forecast"
RPT_DIR = TXO_ROOT / "reports"
STATE = TXO_ROOT / "logs" / "morning_state.json"
HISTORY = TXO_ROOT / "logs" / "morning_history.jsonl"
LOGDIR = TXO_ROOT / "logs"

MB, ME = "<!-- MORNING-BEGIN -->", "<!-- MORNING-END -->"
OB, OE = "<!-- OPEN-BEGIN -->", "<!-- OPEN-END -->"
SLOT = "<!-- MORNING-SLOT -->"
OSLOT = "<!-- OPEN-SLOT -->"


class Abort(Exception):
    """失敗(要計數、要吵)。與 no-op(休市)分開。"""


# ---------------- 夜盤資料源 ----------------

def _load_pb2():
    p = PLATFORM / "core" / "data_schemas" / "txf_data_pb2.py"
    if not p.exists():
        raise Abort(f"pb2 不存在:{p}(platform repo 未就位?)")
    spec = importlib.util.spec_from_file_location("txf_data_pb2", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.Tick


def night_from_kafka(fc_date):
    """撈 [fc_date 15:00, fc_date+1 05:00) 的 txf-tick,聚 5m。回 dict 或 None(無夜盤)。"""
    from confluent_kafka import Consumer, TopicPartition
    PbTick = _load_pb2()
    t0 = datetime.combine(fc_date, datetime.min.time()).replace(hour=15)
    t1 = t0 + timedelta(hours=14)
    ms0, ms1 = t0.timestamp() * 1000, t1.timestamp() * 1000
    c = Consumer({"bootstrap.servers": KAFKA_BROKER, "group.id": f"txo-morning-{uuid.uuid4()}",
                  "enable.auto.commit": False, "fetch.max.bytes": 52428800,
                  "socket.timeout.ms": 10000})
    try:
        try:
            res = c.offsets_for_times([TopicPartition("txf-tick", 0, int(ms0))], timeout=15)
        except Exception as e:
            raise Abort(f"Kafka 連線失敗:{e}")
        off = res[0].offset
        lo_w, hi_w = c.get_watermark_offsets(TopicPartition("txf-tick", 0), timeout=10)
        if off < 0:
            # 時間點之後沒有任何訊息:整夜無資料(假日)→ no-op;
            # 但若連 watermark 都拿不到會在上面就炸 → 失敗路徑分明。
            return None
        c.assign([TopicPartition("txf-tick", 0, off)])
        rows, empt, hard_cap = [], 0, 800_000
        while len(rows) < hard_cap:
            m = c.poll(2.0)
            if m is None:
                empt += 1
                if empt > 3:
                    break
                continue
            empt = 0
            if m.error():
                continue
            pb = PbTick()
            pb.ParseFromString(m.value())
            if pb.timestamp_ms >= ms1:
                break
            if pb.timestamp_ms >= ms0:
                rows.append((pb.timestamp_ms, pb.close / 10000.0))
            if m.offset() >= hi_w - 1:
                break
        else:
            raise Abort(f"tick 數超過 hard cap {hard_cap},視為異常")
    finally:
        c.close()
    if not rows:
        return None                                            # 無夜盤(假日)
    px = [r[1] for r in rows]
    buck = {}
    for ms, pr in rows:
        b = int(ms // 300000)                                  # 5m 桶(UTC epoch,桶界與台北一致:+8h 整除 5m)
        buck[b] = pr                                           # 桶內最後一筆 = 5m close
    closes = [buck[k] for k in sorted(buck)]
    rv = math.sqrt(sum((math.log(b / a)) ** 2 for a, b in zip(closes, closes[1:]))) if len(closes) > 1 else 0.0
    return {"src": "kafka", "n_ticks": len(rows), "bars": len(closes),
            "O": px[0], "H": max(px), "L": min(px), "C": px[-1],
            "rv_pts": rv * px[-1]}


def night_from_lake(fc_date):
    """歷史重放:讀湖裡 fc_date 的 Night 5m 棒(13:50 sync 後才有)。"""
    try:
        fs = kbar_paths("5m", "TXF", fc_date, fc_date)
        k = (pl.read_parquet(fs).with_columns(pl.col("date").cast(pl.Utf8))
             .filter((pl.col("date") == fc_date.isoformat()) & (pl.col("session") == "Night"))
             .sort("ts"))
    except Exception as e:
        raise Abort(f"讀湖失敗:{e}")
    if not k.height:
        return None
    cl = k["close"].to_numpy()
    rv = math.sqrt(float(((pl.Series(cl).log().diff().drop_nulls()) ** 2).sum()))
    return {"src": "lake", "n_ticks": None, "bars": k.height,
            "O": float(k["open"][0]), "H": float(k["high"].max()),
            "L": float(k["low"].min()), "C": float(cl[-1]), "rv_pts": rv * float(cl[-1])}


def open_from_kafka(target):
    """08:46 用:取 [08:45, 08:46:30) 的最後一筆成交價當 O。"""
    from confluent_kafka import Consumer, TopicPartition
    PbTick = _load_pb2()
    t0 = datetime.combine(target, datetime.min.time()).replace(hour=8, minute=45)
    ms0 = t0.timestamp() * 1000
    ms1 = ms0 + 90 * 1000
    c = Consumer({"bootstrap.servers": KAFKA_BROKER, "group.id": f"txo-open-{uuid.uuid4()}",
                  "enable.auto.commit": False, "socket.timeout.ms": 10000})
    try:
        res = c.offsets_for_times([TopicPartition("txf-tick", 0, int(ms0))], timeout=15)
        off = res[0].offset
        if off < 0:
            return None
        c.assign([TopicPartition("txf-tick", 0, off)])
        last, empt = None, 0
        while True:
            m = c.poll(2.0)
            if m is None:
                empt += 1
                if empt > 3:
                    break
                continue
            empt = 0
            if m.error():
                continue
            pb = PbTick()
            pb.ParseFromString(m.value())
            if pb.timestamp_ms >= ms1:
                break
            if pb.timestamp_ms >= ms0:
                last = pb.close / 10000.0
    finally:
        c.close()
    return last


# ---------------- 計算 ----------------

def sigma_prime(sigma, night):
    """σ′(齊次混合尺)。回 (σ′, 模式字串)。降級判準 = CALIB 的完整性閘。"""
    if night["bars"] >= CALIB["MIN_BARS"]:
        nr = night["rv_pts"] / sigma
        w, med, mode = CALIB["W"], CALIB["NR_MED"], "RV"
    else:
        park = (night["H"] - night["L"]) / (2 * math.sqrt(math.log(2)))
        nr = park / sigma
        w, med, mode = CALIB["W_PARK"], CALIB["NR_MED_PARK"], "PARK-降級"
    return sigma * math.sqrt(1 - w + w * (nr / med) ** 2), mode


def to_mult(pts, F, sigma):
    return (pts - F) / sigma


def build_morning_block(fc, night, target, weekend):
    """組出 MORNING-BEGIN..END 的 HTML。回 (html, record)。"""
    F, sigma = fc["F"], fc["sigma"]
    sp, mode = sigma_prime(sigma, night)
    if weekend:
        sp *= CALIB["MONDAY_X"]
    nC = night["C"]
    M = CALIB["morning"]

    # 計分板:fc 的夜盤預測帶(轉回 σ 倍數畫在 F/σ 座標)+ 實際 H/C/L 菱形
    nb = fc["bands"]["night"]
    lanes_night = [
        ("最高點", "#ef5350", to_mult(nb["hi"][0], F, sigma), to_mult(nb["hi"][1], F, sigma),
         to_mult(nb["hi"][2], F, sigma), None, to_mult(nb["hi"][3], F, sigma)),
        ("收盤", "#7f8ea3", to_mult(nb["cl67"][0], F, sigma), 0.0,
         to_mult(nb["cl67"][1], F, sigma), to_mult(nb["cl90"][0], F, sigma),
         to_mult(nb["cl90"][1], F, sigma)),
        ("最低點", "#26a69a", to_mult(nb["lo"][3], F, sigma), to_mult(nb["lo"][1], F, sigma),
         to_mult(nb["lo"][2], F, sigma), to_mult(nb["lo"][0], F, sigma), None),
    ]
    # 更新後的日盤帶(錨 nC、尺 σ′;畫在同一座標 → 直接轉 σ 倍數)
    day_pts = {
        "hi": [nC + x * sp for x in M["hi"]],
        "lo": [nC + x * sp for x in M["lo"]],
        "cl67": [nC - M["cl67"] * sp, nC + M["cl67"] * sp],
        "cl90": [nC - M["cl90"] * sp, nC + M["cl90"] * sp],
    }
    ob = fc["bands"]["day"]
    lanes_day = [
        ("最高點", "#ef5350", to_mult(day_pts["hi"][0], F, sigma), to_mult(day_pts["hi"][1], F, sigma),
         to_mult(day_pts["hi"][2], F, sigma), None, to_mult(day_pts["hi"][3], F, sigma)),
        ("收盤", "#7f8ea3", to_mult(day_pts["cl67"][0], F, sigma),
         to_mult(nC, F, sigma), to_mult(day_pts["cl67"][1], F, sigma),
         to_mult(day_pts["cl90"][0], F, sigma), to_mult(day_pts["cl90"][1], F, sigma)),
        ("最低點", "#26a69a", to_mult(day_pts["lo"][3], F, sigma), to_mult(day_pts["lo"][1], F, sigma),
         to_mult(day_pts["lo"][2], F, sigma), to_mult(day_pts["lo"][0], F, sigma), None),
        ("14:25 版收盤帶", "#5a6470", to_mult(ob["cl67"][0], F, sigma), 0.0,
         to_mult(ob["cl67"][1], F, sigma), to_mult(ob["cl90"][0], F, sigma),
         to_mult(ob["cl90"][1], F, sigma)),
    ]
    svg = _svg_lanes(F, sigma, [
        {"label": "昨晚夜盤", "time": "預測帶 vs 實際(◆)", "lanes": lanes_night,
         "marks": [(0, night["H"]), (1, night["C"]), (2, night["L"])]},
        {"label": "今日日盤", "time": "錨 = 夜盤收盤 · 尺 = σ′", "lanes": lanes_day},
    ])

    old_w = ob["cl67"][1] - ob["cl67"][0]
    new_w = day_pts["cl67"][1] - day_pts["cl67"][0]
    hits = [nb["hi"][0] <= night["H"] <= nb["hi"][2],
            nb["cl67"][0] <= night["C"] <= nb["cl67"][1],
            nb["lo"][3] <= night["L"] <= nb["lo"][2]]
    score = "".join("✅" if h else "❌" for h in hits)
    warn = ""
    if weekend:
        warn = ("<p class='warn' style='margin:6px 0 0'>⚠️ 跨週末:帶寬已 ×1.13。"
                "開盤後請改用下方換算卡(間隔越長,卡的優勢越大)。</p>")
    if mode != "RV":
        warn += (f"<p class='neg' style='margin:6px 0 0'>⚠️ 夜盤 5m 只有 {night['bars']}/"
                 f"{CALIB['EXP_BARS']} 根 → 已降級為振幅估計(Parkinson,W=0.4)。</p>")

    card = _card_html("開盤換算卡(σ′ 版)", CALIB["card_sprime"], sp,
                      "已含昨晚夜盤實況;直接把開盤價填進去")
    block = (
        f"{MB}\n<div class='panel' style='border-color:#f5d90a;background:#141a12'>"
        f"<b style='font-size:20px'>🌅 早報 {target}</b>"
        f"<span class='mut'> — {datetime.now().strftime('%m-%d %H:%M')} 產,"
        f"夜盤實況 + 日盤重新錨定(14:25 那份原樣保留在下方)</span>"
        f"<div style='margin-top:6px;font-size:16px'>夜盤收在 <b>{nC:,.0f}</b>"
        f"({nC - F:+,.0f})· 高 {night['H']:,.0f} / 低 {night['L']:,.0f} · "
        f"計分 高{score[0]} 收{score[1]} 低{score[2]} · "
        f"σ′ = <b>{sp:,.0f}</b> 點(σ 的 {sp / sigma:.2f} 倍)· "
        f"收盤 67% 帶寬 &plusmn;{old_w / 2:,.0f} &rarr; <b>&plusmn;{new_w / 2:,.0f}</b>"
        f"({(new_w / old_w - 1) * 100:+.0f}%)</div>"
        + svg + warn + card + f"\n{OSLOT}\n"
        + f"<p class='mut' style='margin:6px 0 0'>來源 fc_{fc['date'].replace('-', '')}.json"
        f"(14:25 凍結)· 夜盤 5m {night['bars']}/{CALIB['EXP_BARS']} 根"
        f"({night['src']})· 夜盤 RV {night['rv_pts']:,.0f} 點 · 模式 {mode}</p>"
        f"</div>\n{ME}")
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "target": target,
           "fc": fc, "night": night, "sigma_prime": round(sp, 2), "mode": mode,
           "weekend": weekend, "day_bands": {k: [round(x, 1) for x in v]
                                             for k, v in day_pts.items()}}
    return block, rec


def build_open_block(O, sp, spec_key):
    spec = CALIB[spec_key]
    hi, lo = spec["hi"], spec["lo"]
    g = lambda x: f"{O + x * sp:,.0f}"
    return (
        f"{OB}\n<div class='panel' style='border-color:#26a69a;background:#10181a'>"
        f"<b style='font-size:18px'>☀️ 開盤定稿 {datetime.now().strftime('%H:%M')}</b>"
        f"<span class='mut'> — O = <b>{O:,.0f}</b>(08:45–08:46 末筆),今日最終區間</span>"
        f"<div style='margin-top:6px;font-size:17px;line-height:1.9'>"
        f"最高 67% <b>{g(hi[0])} ~ {g(hi[2])}</b>(中位 {g(hi[1])})· 90% {g(hi[3])}<br>"
        f"最低 67% <b>{g(lo[3])} ~ {g(lo[2])}</b>(中位 {g(lo[1])})· 90% {g(lo[0])}<br>"
        f"收盤 67% <b>{O - spec['cl67'] * sp:,.0f} – {O + spec['cl67'] * sp:,.0f}</b>"
        f" · 90% {O - spec['cl90'] * sp:,.0f} – {O + spec['cl90'] * sp:,.0f}"
        f"</div></div>\n{OE}")


# ---------------- 檔案操作(冪等)----------------

def splice(html, block, begin, end, slot):
    if begin in html and end in html:
        i, j = html.index(begin), html.index(end) + len(end)
        return html[:i] + block + html[j:]
    if slot in html:
        return html.replace(slot, slot + "\n" + block, 1)
    raise Abort(f"報表裡找不到 {slot} 也沒有既有區塊 —— 版型不符,拒絕硬插")


def write_report(fc_date, block, begin=MB, end=ME, slot=SLOT):
    fp = RPT_DIR / f"gex_{fc_date.strftime('%Y%m%d')}.html"
    if not fp.exists():
        raise Abort(f"{fp.name} 不存在")
    html = splice(fp.read_text(encoding="utf-8"), block, begin, end, slot)
    fp.write_text(html, encoding="utf-8")
    newest = max((f.stem[4:] for f in RPT_DIR.glob("gex_*.html")), default="")
    if fc_date.strftime("%Y%m%d") >= newest:      # latest 防倒退(同一條鐵律)
        (RPT_DIR / "latest.html").write_text(html, encoding="utf-8")
    return fp


def write_state(ok, note, extra=None):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    try:
        st = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        st = {}
    fails = 0 if ok else st.get("consecutive_failures", 0) + 1
    st.update({"last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "last_ok": ok, "consecutive_failures": fails, "note": note})
    if extra:
        st.update(extra)
    STATE.write_text(json.dumps(st, ensure_ascii=False, indent=1), encoding="utf-8")


def append_history(rec):
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def find_fc(target):
    """最新一份日期 < target 的 fc。回 (fc dict, fc_date, gap_days)。"""
    cands = sorted(FC_DIR.glob("fc_*.json"))
    tgt = target.strftime("%Y%m%d")
    prev = [f for f in cands if f.stem[3:] < tgt]
    if not prev:
        raise Abort("找不到任何早於目標日的 fc_*.json(14:25 那輪沒跑?)")
    fp = prev[-1]
    fc = json.loads(fp.read_text(encoding="utf-8"))
    fc_date = date.fromisoformat(fc["date"])
    gap = (target - fc_date).days
    if gap > 5:
        raise Abort(f"fc 最新只有 {fc_date}(距目標 {gap} 天)—— 14:25 那輪斷了,先修它")
    return fc, fc_date, gap


# ---------------- 主流程 ----------------

def run_night(target, source, dry):
    fc, fc_date, gap = find_fc(target)
    night = (night_from_kafka if source == "kafka" else night_from_lake)(fc_date)
    if night is None:
        print(f"[NOOP] {fc_date} 無夜盤(假日)→ 不更新,層 1 的 σ 版換算卡就是為這種日子準備的")
        write_state(True, f"no night session after {fc_date}")
        return True
    weekend = gap >= 3
    block, rec = build_morning_block(fc, night, target.isoformat(), weekend)
    if dry:
        print(json.dumps({k: v for k, v in rec.items() if k != "fc"},
                         ensure_ascii=False, indent=1))
        return True
    fp = write_report(fc_date, block)
    append_history(rec)
    write_state(True, "", {"last_target": target.isoformat(),
                           "sigma_prime": rec["sigma_prime"], "mode": rec["mode"],
                           "bars": night["bars"], "fc_date": fc_date.isoformat()})
    print(f"[OK] 早報 {target} ← 夜盤{fc_date}({night['bars']}/{CALIB['EXP_BARS']} 根,"
          f"{rec['mode']})σ′={rec['sigma_prime']:,.0f} → {fp.name}"
          + (" [跨週末×1.13]" if weekend else ""))
    return True


def run_open(target, dry):
    try:
        st = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        st = {}
    O = open_from_kafka(target)
    if O is None:
        raise Abort("抓不到 08:45–08:46 的成交價(還沒開盤?Kafka 斷線?)")
    if st.get("last_target") == target.isoformat() and st.get("sigma_prime"):
        sp, key = float(st["sigma_prime"]), "card_sprime"
        fc_date = date.fromisoformat(st["fc_date"])
    else:                                       # 連假後沒有早報 → 退回 σ 版
        fc, fc_date, _ = find_fc(target)
        sp, key = fc["sigma"], "card_sigma"
    block = build_open_block(O, sp, key)
    if dry:
        print(f"O={O:,.0f} σ={sp:,.0f} spec={key}")
        return True
    fp = write_report(fc_date, block, begin=OB, end=OE, slot=OSLOT)
    print(f"[OK] 開盤定稿 {target} O={O:,.0f}({key})→ {fp.name}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="目標交易日 YYYY-MM-DD(預設:今天;週六→下週一)")
    ap.add_argument("--mode", choices=["night", "open"], default="night")
    ap.add_argument("--source", choices=["kafka", "lake"], default="kafka")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    LOGDIR.mkdir(parents=True, exist_ok=True)
    lp = LOGDIR / f"morning-{date.today()}.log"

    class _Tee:
        def __init__(self, s, p):
            self.s, self.f = s, open(p, "a", encoding="utf-8")

        def write(self, x):
            self.s.write(x)
            self.f.write(x)
            self.f.flush()

        def flush(self):
            self.s.flush()

    if not a.dry_run:
        sys.stdout = _Tee(sys.stdout, lp)
    print(f"===== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} txo_morning "
          f"mode={a.mode} source={a.source} =====")

    if a.date:
        target = date.fromisoformat(a.date)
    else:
        target = date.today()
        if target.weekday() == 5:                      # 週六 → 目標下週一
            target += timedelta(days=2)
        elif target.weekday() == 6:
            print("[NOOP] 週日不跑")
            return 0
    try:
        ok = run_night(target, a.source, a.dry_run) if a.mode == "night" \
            else run_open(target, a.dry_run)
        return 0 if ok else 1
    except Abort as e:
        print(f"[FAIL] {e}")
        if not a.dry_run:
            write_state(False, str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
