# -*- coding: utf-8 -*-
"""TXO 早報(05:05)—— 夜盤實況計分 + 明日日盤重新錨定,寫回**同一份**報表。

用法:
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py                     # 排程(Kafka)
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py --date 2026-08-31 --source lake  # 歷史重放
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_morning.py --mode open         # 08:46 第三層
  加 --dry-run 只算不寫(含 state 都不寫)。

設計(2026-09-01,使用者拍板「檔案視同一份、內容新增」):
  ‧ 14:25 的預測由 write_forecast() 凍結在 forecast/fc_D.json —— 本腳本**只讀不重算**,
    「昨天預測了什麼」不受日後改碼影響,計分板才有意義。
  ‧ HTML 更新走 marker 置換(冪等):OPEN-SLOT 與 MORNING-SLOT 都在 14:25 的基底模板裡
    (兩個獨立插槽,開盤定稿不依賴早報存在 —— 連假後早報 no-op 時 σ 版開盤定稿仍能落地),
    置換各自的 BEGIN..END,重跑不會疊兩份;層 1 一個位元組都不動。寫檔走 tmp+os.replace(原子)。
  ‧ 依據(session 量測,詳 CALIB 註解與 wiki/Range-Forecast.md):
      - 夜盤只搬錨點不帶方向(r=+0.058);錨改夜盤收盤 → 帶寬合計 5.04σ→3.15σ。
      - 夜盤振幅預告日盤振幅 → σ′ = σ·√(1−W+W·(nr/NR_MED)²),W=0.6。
      - 開盤價是開盤前一切的充分統計量 → 開盤換算卡;連假後夜盤錨失準(蓋 37%)
        而開盤錨不受影響(70%)⇒ MONDAY_X 只乘夜盤錨的日盤帶,**不乘**開盤錨的卡與定稿。
  ‧ 失效哲學:**絕不用陳舊資料硬畫一份看起來正常的早報**。
      夜盤 5m ≥150/168 → RV(W=0.6);棒數不足但有高低 → Parkinson 降級(W=0.4,標明)
      夜盤讀取中斷(未達水位也未達 05:00)/ 資料尾端離 05:00 >30 分鐘 → **失敗**
      整夜 0 筆且 producer 當天日盤還活著 → 真休市 no-op;producer 看起來整天沒活 → **失敗**
      fc 與 target 之間夾著交易日(14:25 斷了)→ **失敗**;純長連假(gap>5)→ no-op 並指路 σ 版卡
      任何未預期例外 → traceback 進日誌 + write_state(False),絕不無聲
  ‧ 健康:logs/morning_state.json(night/open 分開計數);14:25 的 txo_gex_daily 讀它告警。
    不可變稽核 = logs/morning_history.jsonl。

⚠ 時戳陷阱(兩個,別搞混):
  ① 本機時區=台北 ⇒ naive datetime 直接 .timestamp(),**不可再手動 −8h**
    (2026-08-31 犯過:減兩次 seek 到日盤開盤)。
  ② pb.timestamp_ms 與 broker ts 都是正規 UTC epoch ms;轉台北 = fromtimestamp(utc)+8h。
    (Shioaji api.ticks 的「台北當 UTC」陷阱是**另一回事**,不在 Kafka 這條路上。)
"""
import sys
import os
import json
import math
import time
import uuid
import argparse
import traceback
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
                           _lane_legend)
from config.lake_paths import kbar_paths                       # noqa: E402

PLATFORM = _HERE.parent / "txf-quant-platform"
KAFKA_BROKER = "192.168.1.50:9092"          # 與 platform config 同值;搬 broker 要一起改
FC_DIR = TXO_ROOT / "forecast"
RPT_DIR = TXO_ROOT / "reports"
STATE = TXO_ROOT / "logs" / "morning_state.json"
HISTORY = TXO_ROOT / "logs" / "morning_history.jsonl"
LOGDIR = TXO_ROOT / "logs"

MB, ME = "<!-- MORNING-BEGIN -->", "<!-- MORNING-END -->"
L1B, L1E = "<!-- L1-BEGIN -->", "<!-- L1-END -->"
L0B, L0E = "<!-- L0-BEGIN -->", "<!-- L0-END -->"
OB, OE = "<!-- OPEN-BEGIN -->", "<!-- OPEN-END -->"
SLOT = "<!-- MORNING-SLOT -->"
OSLOT = "<!-- OPEN-SLOT -->"
TPE_UTC_OFF = timedelta(hours=8)
STALE_MIN = 30                               # 夜盤資料尾端離 05:00 超過此分鐘數 = 陳舊,拒畫


class Abort(Exception):
    """失敗(要計數、要吵)。與 no-op(休市)分開。"""


def _tpe(ms):
    return datetime.utcfromtimestamp(ms / 1000) + TPE_UTC_OFF


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
    """撈 [fc_date 15:00, fc_date+1 05:00) 的 txf-tick,聚 5m。

    回 dict(含 last_ts)或 None(**確認過的**無夜盤)。
    分辨「休市」與「producer 死了」:0 筆時看 topic 最新一筆的時戳 ——
    producer 若當天日盤還活著(最新 ≥ fc_date 13:00)而夜盤零筆,才是真休市。
    讀取中斷(既沒讀到 05:00 也沒追到水位)→ Abort,不把半夜當整夜。
    """
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
            off = res[0].offset
            lo_w, hi_w = c.get_watermark_offsets(TopicPartition("txf-tick", 0), timeout=10)
        except Abort:
            raise
        except Exception as e:
            raise Abort(f"Kafka 連線/查詢失敗:{e!r}")

        def _last_topic_ts():
            """topic 最新一筆的台北時刻(讀不到回 None)。"""
            if hi_w <= lo_w:
                return None
            c.assign([TopicPartition("txf-tick", 0, hi_w - 1)])
            for _ in range(8):
                m = c.poll(1.5)
                if m is not None and not m.error():
                    pb = PbTick()
                    pb.ParseFromString(m.value())
                    return _tpe(pb.timestamp_ms)
            return None

        day_alive_cut = datetime.combine(fc_date, datetime.min.time()).replace(hour=13)

        if off < 0:                                    # 15:00 之後沒有任何訊息
            last = _last_topic_ts()
            if last is not None and last >= day_alive_cut:
                return None                            # 日盤還在動、夜盤零筆 → 真休市
            raise Abort(f"整夜 0 筆且 topic 最新訊息停在 {last} —— 疑 producer 斷線/保留期問題,"
                        f"不能當休市")

        c.assign([TopicPartition("txf-tick", 0, off)])
        rows, empt, hard_cap = [], 0, 800_000
        last_off, reached_end = -1, False
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
            last_off = m.offset()
            if pb.timestamp_ms >= ms1:
                reached_end = True
                break
            if pb.timestamp_ms >= ms0:
                rows.append((pb.timestamp_ms, pb.close / 10000.0))
            if last_off >= hi_w - 1:
                reached_end = True
                break
        else:
            raise Abort(f"tick 數超過 hard cap {hard_cap},視為異常")

        if not rows:
            last = _last_topic_ts()
            if last is not None and last >= day_alive_cut:
                return None
            raise Abort(f"夜盤視窗 0 筆且 topic 最新訊息停在 {last} —— 疑 producer 斷線")
        if not reached_end:
            raise Abort(f"夜盤讀取中斷:只到 {_tpe(rows[-1][0]):%H:%M:%S}"
                        f"(offset {last_off}/{hi_w - 1}),不把半夜當整夜")
    finally:
        c.close()
    px = [r[1] for r in rows]
    buck = {}
    for ms, pr in rows:
        buck[int(ms // 300000)] = pr                   # 5m 桶(+8h 整除 5m,桶界與台北一致)
    closes = [buck[k] for k in sorted(buck)]
    rv = (math.sqrt(sum(math.log(b / a) ** 2 for a, b in zip(closes, closes[1:])))
          if len(closes) > 1 else 0.0)
    return {"src": "kafka", "n_ticks": len(rows), "bars": len(closes),
            "O": px[0], "H": max(px), "L": min(px), "C": px[-1],
            "rv_pts": rv * px[-1],
            "last_ts": _tpe(rows[-1][0]).strftime("%Y-%m-%d %H:%M:%S")}


def night_from_lake(fc_date):
    """歷史重放:讀湖裡 fc_date 的 Night 5m 棒(13:50 sync 後才有)。"""
    try:
        fs = kbar_paths("5m", "TXF", fc_date, fc_date)
        k = (pl.read_parquet(fs).with_columns(pl.col("date").cast(pl.Utf8))
             .filter((pl.col("date") == fc_date.isoformat()) & (pl.col("session") == "Night"))
             .sort("ts"))
    except Exception as e:
        raise Abort(f"讀湖失敗:{e!r}")
    if not k.height:
        return None
    cl = k["close"].to_numpy()
    rv = math.sqrt(float(((pl.Series(cl).log().diff().drop_nulls()) ** 2).sum()))
    last_bar_end = k["ts"][-1] + timedelta(minutes=5)     # 棒首 ts + 5m = 資料尾端
    return {"src": "lake", "n_ticks": None, "bars": k.height,
            "O": float(k["open"][0]), "H": float(k["high"].max()),
            "L": float(k["low"].min()), "C": float(cl[-1]), "rv_pts": rv * float(cl[-1]),
            "last_ts": last_bar_end.strftime("%Y-%m-%d %H:%M:%S")}


def check_freshness(fc_date, night):
    """夜盤資料尾端必須貼近 05:00 —— 錨陳舊就拒畫(檔頭失效哲學)。"""
    end = datetime.combine(fc_date + timedelta(days=1), datetime.min.time()).replace(hour=5)
    last = datetime.strptime(night["last_ts"], "%Y-%m-%d %H:%M:%S")
    gap_min = (end - last).total_seconds() / 60
    if gap_min > STALE_MIN:
        raise Abort(f"夜盤資料只到 {night['last_ts']}(離 05:00 差 {gap_min:.0f} 分鐘)"
                    f"—— 錨陳舊,拒絕出報")


def open_from_kafka(target):
    """取 08:45:00 之後的**第一筆**成交 = 集合競價開盤價。

    2026-09-01 改為**有界等待**(使用者:開盤價 08:45 就有,別拖):排程可排 08:45,
    查不到就每 2 秒重查,最多等到 08:46:15(晚跑另有 75 秒寬限)—— 競價 tick 一進
    Kafka 立刻出報;假日等完照樣回 None(no-op)。單發不重試的舊版會把
    「排程比 tick 早半秒」變成整天缺定稿。

    2026-09-01 修正:原版取 08:45–08:46:30 的末筆(「你螢幕上的價」),但
    ① CALIB 的開盤錨係數是拿**真開盤價**校準的(band_common 的 1d Day open),
    ② 使用者看盤軟體顯示的「開盤價」就是這筆 —— 印別的數字徒增困惑,
    ③ 兩者差距中位 18 點、P90 75 點、最大 313 點(n=642)—— 尾巴上不可忽略。"""
    from confluent_kafka import Consumer, TopicPartition
    PbTick = _load_pb2()
    t0 = datetime.combine(target, datetime.min.time()).replace(hour=8, minute=45)
    ms0 = t0.timestamp() * 1000
    ms1 = ms0 + 90 * 1000
    c = Consumer({"bootstrap.servers": KAFKA_BROKER, "group.id": f"txo-open-{uuid.uuid4()}",
                  "enable.auto.commit": False, "socket.timeout.ms": 10000})
    try:
        deadline = max(time.time() + 75, ms0 / 1000 + 90)   # 早跑等到 08:46:15;晚跑寬限 75s
        while True:
            try:
                res = c.offsets_for_times([TopicPartition("txf-tick", 0, int(ms0))], timeout=15)
            except Exception as e:
                raise Abort(f"Kafka 連線/查詢失敗:{e!r}")
            off = res[0].offset
            if off >= 0:
                break
            if time.time() >= deadline:
                return None                       # 等完仍無 ≥08:45 成交 → 假日/未開盤
            time.sleep(2)
        c.assign([TopicPartition("txf-tick", 0, off)])
        first, empt = None, 0
        while first is None:
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
            if pb.timestamp_ms >= ms1:      # 視窗內沒有成交(不該發生)
                break
            if pb.timestamp_ms >= ms0:
                first = pb.close / 10000.0  # 第一筆 = 開盤價
    finally:
        c.close()
    return first


def open_from_lake(target):
    """歷史重放:湖裡 target 日盤第一根 1m 的 **open** = 集合競價開盤價(與 Kafka 版同義)。"""
    try:
        fs = kbar_paths("1m", "TXF", target, target)
        k = (pl.read_parquet(fs).with_columns(pl.col("date").cast(pl.Utf8))
             .filter((pl.col("date") == target.isoformat()) & (pl.col("session") == "Day"))
             .sort("ts"))
    except Exception as e:
        raise Abort(f"讀湖失敗:{e!r}")
    return float(k["open"][0]) if k.height else None    # 首根 open ≡ 集合競價開盤(已驗 ≡ 1d open)


def trading_days_between(d0, d1):
    """(d0, d1) **開區間**內湖裡有日盤的日子。抓「fc 與 target 中間漏了交易日」。"""
    if (d1 - d0).days <= 1:
        return []
    try:
        fs = kbar_paths("1d", "TXF", d0 + timedelta(days=1), d1 - timedelta(days=1))
        if not fs:
            return []
        k = (pl.read_parquet(fs).with_columns(pl.col("date").cast(pl.Utf8))
             .filter((pl.col("session") == "Day")
                     & (pl.col("date") > d0.isoformat())
                     & (pl.col("date") < d1.isoformat())))
        return sorted(k["date"].unique().to_list())
    except Exception:
        return []                                     # 探針壞了別擋路;fc 斷檔另有 gap 檢查


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
    """組出 MORNING-BEGIN..END 的 HTML。回 (html, record)。

    MONDAY_X 只乘**夜盤錨的日盤帶**(它是對這組量校準的);
    開盤錨的卡與 state 存的 sigma_prime 用未乘版 —— 開盤錨連假後都不受影響(CALIB 註解),
    週末更不需要放寬(2026-09-01 審查修正:原版外溢到卡,重複放寬 13%)。
    """
    F, sigma = fc["F"], fc["sigma"]
    sp_raw, mode = sigma_prime(sigma, night)
    sp_day = sp_raw * CALIB["MONDAY_X"] if weekend else sp_raw
    nC = night["C"]
    M = CALIB["morning"]

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
    day_pts = {
        "hi": [nC + x * sp_day for x in M["hi"]],
        "lo": [nC + x * sp_day for x in M["lo"]],
        "cl67": [nC - M["cl67"] * sp_day, nC + M["cl67"] * sp_day],
        "cl90": [nC - M["cl90"] * sp_day, nC + M["cl90"] * sp_day],
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
        ("舊收盤帶", "#5a6470", to_mult(ob["cl67"][0], F, sigma), 0.0,
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
        warn = ("<p class='warn' style='margin:6px 0 0'>⚠️ 跨週末:日盤帶寬已 ×1.13"
                "(開盤換算卡不乘 —— 開盤錨不受間隔影響)。開盤後以卡為準。"
                "若週一適逢休市,本更新不適用。</p>")
    if mode != "RV":
        warn += (f"<p class='neg' style='margin:6px 0 0'>⚠️ 夜盤 5m 只有 {night['bars']}/"
                 f"{CALIB['EXP_BARS']} 根 → 已降級為振幅估計(Parkinson,W=0.4)。</p>")

    card = _card_html("開盤換算卡(σ′ 版)", CALIB["card_sprime"], sp_raw,
                      "已含昨晚夜盤實況;直接把開盤價填進去")
    block = (
        f"{MB}\n<div class='panel' style='border-color:#f5d90a;background:#141a12'>"
        f"<b style='font-size:20px'>🌅 早報 {target}</b>"
        f"<span class='mut'> — {datetime.now().strftime('%m-%d %H:%M')} 產,"
        f"夜盤實況 + 日盤重新錨定(14:25 那份原樣保留在下方)</span>"
        f"<div style='margin-top:6px;font-size:16px'>夜盤收在 <b>{nC:,.0f}</b>"
        f"({nC - F:+,.0f})· 高 {night['H']:,.0f} / 低 {night['L']:,.0f} · "
        f"計分 高{score[0]} 收{score[1]} 低{score[2]} · "
        f"σ′ = <b>{sp_raw:,.0f}</b> 點(σ 的 {sp_raw / sigma:.2f} 倍)· "
        f"收盤 67% 帶寬 &plusmn;{old_w / 2:,.0f} &rarr; <b>&plusmn;{new_w / 2:,.0f}</b>"
        f"({(new_w / old_w - 1) * 100:+.0f}%)</div>"
        + svg + _lane_legend(actual=True, oldband=True) + warn + card
        + f"<p class='mut' style='margin:6px 0 0'>來源 fc_{fc['date'].replace('-', '')}.json"
        f"(14:25 凍結)· 夜盤 5m {night['bars']}/{CALIB['EXP_BARS']} 根"
        f"({night['src']},至 {night['last_ts'][11:16]})· 夜盤 RV {night['rv_pts']:,.0f} 點"
        f" · 模式 {mode}</p>"
        f"</div>\n{ME}")
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "target": target,
           "fc": fc, "night": night, "sigma_prime": round(sp_raw, 2), "mode": mode,
           "weekend": weekend, "day_bands": {k: [round(x, 1) for x in v]
                                             for k, v in day_pts.items()}}
    return block, rec


def build_open_block(O, sp, spec_key):
    spec = CALIB[spec_key]
    hi, lo = spec["hi"], spec["lo"]
    lanes = [("最高點", "#ef5350", hi[0], hi[1], hi[2], None, hi[3]),
             ("收盤", "#7f8ea3", -spec["cl67"], 0.0, spec["cl67"],
              -spec["cl90"], spec["cl90"]),
             ("最低點", "#26a69a", lo[3], lo[1], lo[2], lo[0], None)]
    svg = _svg_lanes(O, sp, [{"label": "今日日盤", "time": f"錨 = 開盤 {O:,.0f}",
                              "lanes": lanes}])
    g = lambda x: f"{O + x * sp:,.0f}"
    return (
        f"{OB}\n<div class='panel' style='border-color:#26a69a;background:#10181a'>"
        f"<b style='font-size:18px'>☀️ 開盤定稿 {datetime.now().strftime('%H:%M')}</b>"
        f"<span class='mut'> — O = <b>{O:,.0f}</b>(08:45 開盤價),今日最終區間"
        f"({'σ′ 版' if spec_key == 'card_sprime' else 'σ 版(無早報,連假後備援)'})</span>"
        f"<div style='margin-top:6px;font-size:17px;line-height:1.9'>"
        f"最高 67% <b>{g(hi[0])} ~ {g(hi[2])}</b>(中位 {g(hi[1])})· 90% {g(hi[3])}<br>"
        f"最低 67% <b>{g(lo[3])} ~ {g(lo[2])}</b>(中位 {g(lo[1])})· 90% {g(lo[0])}<br>"
        f"收盤 67% <b>{O - spec['cl67'] * sp:,.0f} – {O + spec['cl67'] * sp:,.0f}</b>"
        f" · 90% {O - spec['cl90'] * sp:,.0f} – {O + spec['cl90'] * sp:,.0f}"
        f"</div>" + svg + _lane_legend() + f"</div>\n{OE}")


# ---------------- 檔案操作(冪等 + 原子)----------------

def _collapse_span(html, b, e, summary):
    """把 b..e 標記之間的內容包進 <details>(冪等:已包過就跳過)。

    2026-09-01 使用者拍板:任何時刻只有最新一層展開 —— 早報落地收合 14:25 的 ①;
    開盤定稿落地再收合早報。層 1 重生成會自然回到展開,重放後再收合,天然冪等。
    """
    if b not in html or e not in html:
        return html
    i, j = html.index(b) + len(b), html.index(e)
    inner = html[i:j]
    if inner.lstrip().startswith("<details"):
        return html
    return (html[:i]
            + "\n<details style='margin:10px 0'><summary style='cursor:pointer;"
              "color:#9aa3ad;font-size:16px'>" + summary + "</summary>"
            + inner + "</details>\n" + html[j:])


SUM_L0 = "⓪ 今日日盤回顧(開盤定稿 vs 實際)— 點開對照"
SUM_L1 = "📋 14:25 收盤版預測(今晚夜盤 + 明日日盤)— 已被上方更新取代,點開對照"
SUM_MORNING = "🌅 05:05 早報(夜盤計分板 + 日盤重錨)— 已被開盤定稿取代,點開對照"


def splice(html, block, begin, end, slot):
    nb, ne = html.count(begin), html.count(end)
    if nb != ne or nb > 1:
        raise Abort(f"marker 不成對({begin} ×{nb} / {end} ×{ne})—— 疑似殘檔,"
                    f"請用 --report-only 重生該日報表")
    if nb == 1:
        i, j = html.index(begin), html.index(end) + len(end)
        return html[:i] + block + html[j:]
    if slot in html:
        return html.replace(slot, slot + "\n" + block, 1)
    raise Abort(f"報表裡找不到 {slot} 也沒有既有區塊 —— 版型不符(舊版報表?),"
                f"先用 --report-only 重生該日報表")


def write_report(fc_date, block, begin=MB, end=ME, slot=SLOT, collapse=()):
    fp = RPT_DIR / f"gex_{fc_date.strftime('%Y%m%d')}.html"
    if not fp.exists():
        raise Abort(f"{fp.name} 不存在")
    html = splice(fp.read_text(encoding="utf-8"), block, begin, end, slot)
    if "l0" in collapse:
        html = _collapse_span(html, L0B, L0E, SUM_L0)
    if "l1" in collapse:
        html = _collapse_span(html, L1B, L1E, SUM_L1)
    if "morning" in collapse:
        html = _collapse_span(html, MB, ME, SUM_MORNING)
    tmp = fp.with_suffix(".tmp")                      # 原子寫:中斷不留半份殘檔
    tmp.write_text(html, encoding="utf-8")
    os.replace(tmp, fp)
    newest = max((f.stem[4:] for f in RPT_DIR.glob("gex_*.html")), default="")
    if fc_date.strftime("%Y%m%d") >= newest:          # latest 防倒退(同一條鐵律)
        (RPT_DIR / "latest.html").write_text(html, encoding="utf-8")
    return fp


def write_state(ok, note, mode="night", extra=None):
    """night 與 open 各自計數 —— 共用一個計數器會讓 open 的失敗永遠到不了告警門檻
    (每天 05:05 的成功把它歸零)。"""
    STATE.parent.mkdir(parents=True, exist_ok=True)
    try:
        st = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        st = {}
    key = "consecutive_failures" if mode == "night" else "open_failures"
    st[key] = 0 if ok else st.get(key, 0) + 1
    st.update({"last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "last_mode": mode, "last_ok": ok, "note": note})
    if extra:
        st.update(extra)
    STATE.write_text(json.dumps(st, ensure_ascii=False, indent=1), encoding="utf-8")


def append_history(rec):
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def find_fc(target):
    """最新一份日期 < target 的 fc。回 (fc dict, fc_date, gap_days)。

    gap 的裁決在 run_night(要配合湖探針分辨「連假」與「14:25 斷了」),不在這裡。
    """
    cands = sorted(FC_DIR.glob("fc_*.json"))
    tgt = target.strftime("%Y%m%d")
    prev = [f for f in cands if f.stem[3:] < tgt]
    if not prev:
        raise Abort("找不到任何早於目標日的 fc_*.json(14:25 那輪沒跑?)")
    fp = prev[-1]
    try:
        fc = json.loads(fp.read_text(encoding="utf-8"))
        fc_date = date.fromisoformat(fc["date"])
        _ = (fc["F"], fc["sigma"], fc["bands"]["night"], fc["bands"]["day"])
    except Abort:
        raise
    except Exception as e:
        raise Abort(f"{fp.name} 壞檔或舊格式:{e!r}")
    return fc, fc_date, (target - fc_date).days


# ---------------- 主流程 ----------------

def run_night(target, source, dry):
    fc, fc_date, gap = find_fc(target)
    missed = trading_days_between(fc_date, target)
    if missed:
        raise Abort(f"fc 停在 {fc_date},但 {missed} 是有日盤的交易日 —— "
                    f"14:25 那輪斷了,先修它(早報拒絕跳過交易日硬畫)")
    if gap > 5:                                       # 純長連假(中間無交易日)
        print(f"[NOOP] 距上一份 fc({fc_date})已 {gap} 天(長連假)—— 早報帶只對 gap≤3 校準,"
              f"不出報;開盤後用 14:25 那份的 σ 版換算卡(連假後開盤錨覆蓋 70.5% vs 夜盤錨 37.1%)")
        if not dry:
            write_state(True, f"long holiday gap={gap}, skipped by design")
        return True
    night = (night_from_kafka if source == "kafka" else night_from_lake)(fc_date)
    if night is None:
        print(f"[NOOP] {fc_date} 無夜盤(休市前夕,producer 日盤活著)→ 不更新;"
              f"層 1 的 σ 版換算卡就是為這種日子準備的")
        if not dry:
            write_state(True, f"no night session after {fc_date}")
        return True
    check_freshness(fc_date, night)
    weekend = gap >= 3
    block, rec = build_morning_block(fc, night, target.isoformat(), weekend)
    if dry:
        print(json.dumps({k: v for k, v in rec.items() if k != "fc"},
                         ensure_ascii=False, indent=1))
        return True
    fp = write_report(fc_date, block, collapse=("l0", "l1"))
    append_history(rec)
    write_state(True, "", extra={"last_target": target.isoformat(),
                                 "sigma_prime": rec["sigma_prime"], "mode": rec["mode"],
                                 "bars": night["bars"], "fc_date": fc_date.isoformat()})
    print(f"[OK] 早報 {target} ← 夜盤{fc_date}({night['bars']}/{CALIB['EXP_BARS']} 根,"
          f"{rec['mode']})σ′={rec['sigma_prime']:,.0f} → {fp.name}"
          + (" [跨週末:日盤帶×1.13]" if weekend else ""))
    return True


def _morning_record(target):
    """從不可變稽核 jsonl 找 target 最後一筆成功早報(比 state 可靠:state 只記最後一天,
    亂序重放/隔日補跑都會讓它指錯 —— 2026-09-01 開盤層重打實測全退到 σ 版才抓到)。"""
    try:
        rec = None
        with HISTORY.open(encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("kind") == "open":
                    continue
                if r.get("target") == target.isoformat():
                    rec = r
        return rec
    except OSError:
        return None


def run_open(target, dry, source="kafka"):
    O = (open_from_kafka if source == "kafka" else open_from_lake)(target)
    if O is None:
        # 今日尚無 ≥08:45 成交:假日、還沒開盤、或 producer 未啟。前兩者是常態;
        # 第三種由 13:50 湖缺口告警兜底 —— 這裡 no-op 不計失敗,避免連假湊出假警報
        # (夜盤那支的同款教訓,2026-09-01 上排程前補)。
        print(f"[NOOP] {target} 尚無 08:45 之後的成交(假日/未開盤/producer 未啟)"
              f"→ 不出開盤定稿,不計失敗")
        if not dry:
            write_state(True, f"no open trade yet for {target}", mode="open")
        return True
    rec = _morning_record(target)
    if rec and rec.get("sigma_prime"):
        sp, key = float(rec["sigma_prime"]), "card_sprime"
        fc_date = date.fromisoformat(rec["fc"]["date"])
    else:                                             # 早報沒跑成(連假後 no-op 等)→ σ 版
        fc, fc_date, _gap = find_fc(target)
        sp, key = fc["sigma"], "card_sigma"
    block = build_open_block(O, sp, key)
    if dry:
        print(f"O={O:,.0f} 尺={sp:,.0f} spec={key}")
        return True
    fp = write_report(fc_date, block, begin=OB, end=OE, slot=OSLOT,
                      collapse=("l0", "l1", "morning"))
    append_history({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "kind": "open",
                    "target": target.isoformat(), "O": O, "sigma_used": round(sp, 2),
                    "spec": key, "fc_date": fc_date.isoformat()})
    write_state(True, "", mode="open")
    print(f"[OK] 開盤定稿 {target} O={O:,.0f}({key})→ {fp.name}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="目標交易日 YYYY-MM-DD(預設:今天;週六→下週一)")
    ap.add_argument("--mode", choices=["night", "open"], default="night")
    ap.add_argument("--source", choices=["kafka", "lake"], default="kafka")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

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
        LOGDIR.mkdir(parents=True, exist_ok=True)
        sys.stdout = _Tee(sys.stdout, LOGDIR / f"morning-{date.today()}.log")
    print(f"===== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} txo_morning "
          f"mode={a.mode} source={a.source} =====")

    if a.date:
        target = date.fromisoformat(a.date)
    else:
        target = date.today()
        if target.weekday() == 5:                     # 週六 → 目標下週一
            target += timedelta(days=2)
        elif target.weekday() == 6:
            print("[NOOP] 週日不跑")
            return 0
    try:
        ok = run_night(target, a.source, a.dry_run) if a.mode == "night" \
            else run_open(target, a.dry_run, a.source)
        return 0 if ok else 1
    except Abort as e:
        print(f"[FAIL] {e}")
        if not a.dry_run:
            write_state(False, str(e), mode=a.mode)
        return 1
    except Exception as e:                            # 未預期例外也要進日誌與計數,絕不無聲
        print("[CRASH] 未預期例外:\n" + traceback.format_exc())
        if not a.dry_run:
            try:
                write_state(False, f"unexpected: {e!r}", mode=a.mode)
            except Exception:
                pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
