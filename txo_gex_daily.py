# -*- coding: utf-8 -*-
"""TXO dealer GEX 每日管線(Phase 0 產物)。

抓 TAIFEX 公開 CSV -> 落地 D:/txf-data/txo/ parquet -> 算 GEX(兩版符號)
-> 產出 HTML 儀表板(reports/gex_YYYYMMDD.html + latest.html)。

用法(在 txf-data-lake 下、用本 repo venv):
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_gex_daily.py                    # 今天(資料未出或假日會自動跳過)
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_gex_daily.py --date 2026-07-21
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_gex_daily.py --backfill 2026-06-01 2026-07-21
  PYTHONUTF8=1 .venv/Scripts/python.exe txo_gex_daily.py --date 2026-07-21 --report-only  # 從已存 parquet 重算

設計原則:冪等(已存在即跳過,--force 覆寫)、只寫 D:/txf-data/txo/ 新子樹、
資料未公布時安靜跳過(仿 main_etl 幻影守衛精神)、不碰 Shioaji / .env。
符號問題未實證:報告永遠並列「美股慣例」與「台灣證據版」兩條線。
"""
import sys, io, csv, json, math, time, argparse, urllib.request, urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # 排程/非TTY 下防 cp950

import polars as pl

DATA_ROOT = Path(r"D:\txf-data")
TXO_ROOT = DATA_ROOT / "txo"
UA = {"User-Agent": "Mozilla/5.0"}
MULT = 50.0  # TXO 每點 NT$50


# ---------------- TAIFEX 下載 ----------------

def _post(url, params, retries=3):
    data = urllib.parse.urlencode(params).encode()
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            for enc in ("cp950", "utf-8-sig", "utf-8"):
                try:
                    return raw.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw.decode("cp950", errors="replace")
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5)
    raise RuntimeError(f"download failed after {retries} tries: {url}: {last}")


def fetch_daily_csv(kind, d):
    """kind: 'opt'(TXO) or 'fut'(TX)。回傳 (header, rows) 或 (None, None)=當日無資料。"""
    url = {"opt": "https://www.taifex.com.tw/cht/3/optDataDown",
           "fut": "https://www.taifex.com.tw/cht/3/futDataDown"}[kind]
    txt = _post(url, {"down_type": "1",
                      "commodity_id": "TXO" if kind == "opt" else "TX",
                      "queryStartDate": d.strftime("%Y/%m/%d"),
                      "queryEndDate": d.strftime("%Y/%m/%d")})
    if "<html" in txt[:300].lower():
        return None, None
    rows = [r for r in csv.reader(io.StringIO(txt)) if len(r) > 3]
    if len(rows) < 2:
        return None, None
    return [c.strip() for c in rows[0]], rows[1:]


def fetch_institutional(d):
    """三大法人-選擇權買賣權分計(TXO)。端點格式不保證,失敗回 None(不擋主流程)。"""
    candidates = [
        ("https://www.taifex.com.tw/cht/3/callsAndPutsDateDown",
         {"firstDate": "2001/01/01", "queryStartDate": d.strftime("%Y/%m/%d"),
          "queryEndDate": d.strftime("%Y/%m/%d"), "commodityId": "TXO"}),
        ("https://www.taifex.com.tw/cht/3/callsAndPutsDateDown",
         {"queryStartDate": d.strftime("%Y/%m/%d"),
          "queryEndDate": d.strftime("%Y/%m/%d"), "commodityId": "TXO"}),
    ]
    for url, params in candidates:
        try:
            txt = _post(url, params, retries=1)
        except Exception:  # noqa: BLE001
            continue
        if "<html" in txt[:300].lower() or "臺指" not in txt:
            continue
        rows = [r for r in csv.reader(io.StringIO(txt)) if len(r) > 5]
        hdr = [c.strip() for c in rows[0]]

        def col(key, key2=None):
            for i, c in enumerate(hdr):
                if key in c and (key2 is None or key2 in c):
                    return i
            return -1

        ic_prod, ic_cp, ic_actor = col("商品"), col("權別"), col("身份別")
        ic_bl, ic_sl = col("買方", "未平倉口數"), col("賣方", "未平倉口數")
        if min(ic_prod, ic_cp, ic_actor, ic_bl, ic_sl) < 0:
            continue
        out = []
        for r in rows[1:]:
            if "臺指選擇權" not in r[ic_prod]:
                continue
            v = r[ic_cp].strip().upper()
            try:
                out.append({"date": str(d), "cp": "C" if ("買" in v or v.startswith("C")) else "P",
                            "actor": r[ic_actor].strip(),
                            "long_oi": int(r[ic_bl].replace(",", "") or 0),
                            "short_oi": int(r[ic_sl].replace(",", "") or 0)})
            except ValueError:
                continue
        if out:
            return out
    return None


# ---------------- 到期/定價 ----------------

def nth_wed(y, m, n):
    d1 = date(y, m, 1)
    return date(y, m, 1 + (2 - d1.weekday()) % 7 + 7 * (n - 1))


def nth_weekday(y, m, n, wd):
    """該月第 n 個星期 wd(Mon=0)。"""
    first = date(y, m, 1).weekday()
    return date(y, m, 1 + (wd - first) % 7 + 7 * (n - 1))


_TRADING_DAYS = None


def trading_days():
    """湖裡的 TXF 日盤交易日集合(當台指交易日曆用)。"""
    global _TRADING_DAYS
    if _TRADING_DAYS is None:
        s = set()
        p = DATA_ROOT / "kbars" / "1d" / "TXF"
        if p.exists():
            for f in sorted(p.glob("*.parquet")):
                try:
                    df = pl.read_parquet(f).filter(pl.col("session") == "Day")
                    s |= {str(x) for x in df["date"].to_list()}
                except Exception:  # noqa: BLE001
                    pass
        _TRADING_DAYS = s
    return _TRADING_DAYS


def expiry_of(code):
    """由到期碼推到期日(舊 CSV 沒有「契約到期日」欄時的 fallback)。
    W_n=該月第 n 個星期三、F_n=第 n 個星期五、六碼=月選(第三個星期三)。
    名目日若非交易日(假日),順延至下一個交易日。"""
    code = code.strip().replace(" ", "")
    try:
        if "W" in code:
            ym, n = code.split("W")
            d = nth_weekday(int(ym[:4]), int(ym[4:6]), int(n), 2)
        elif "F" in code:
            ym, n = code.split("F")
            d = nth_weekday(int(ym[:4]), int(ym[4:6]), int(n), 4)
        elif len(code) == 6 and code.isdigit():
            d = nth_weekday(int(code[:4]), int(code[4:6]), 3, 2)
        else:
            return None
    except (ValueError, IndexError):
        return None
    td = trading_days()
    # 假日順延:僅在湖的覆蓋範圍內才做(超出範圍的未來日期無從判斷,順延會把它推歪)
    if td and min(td) <= str(d) <= max(td):
        for _ in range(7):
            if str(d) in td:
                break
            d += timedelta(days=1)
    return d


def npdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def b76(F, K, T, sig, cp):
    sq = sig * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sig * sig * T) / sq
    d2 = d1 - sq
    return F * ncdf(d1) - K * ncdf(d2) if cp == "C" else K * ncdf(-d2) - F * ncdf(-d1)


def iv_solve(F, K, T, price, cp):
    intr = max((F - K) if cp == "C" else (K - F), 0.0)
    if price is None or price <= intr + 0.05:
        return None
    lo, hi = 0.005, 3.0
    for _ in range(64):
        mid = 0.5 * (lo + hi)
        if b76(F, K, T, mid, cp) > price:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def gamma_of(F, K, T, iv):
    sq = iv * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * iv * iv * T) / sq
    return npdf(d1) / (F * sq)


# ---------------- 主流程 ----------------

def num(s):
    s = s.strip().replace(",", "").replace('"', "")
    if s in ("", "-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def taiex_close(d):
    """從資料湖取 TAIEX 日盤收盤(TXO 的真正標的)。取不到回 None。"""
    p = DATA_ROOT / "kbars" / "1d" / "TSE" / f"TSE_1d_{d.year}.parquet"
    if not p.exists():
        return None
    try:
        df = pl.read_parquet(p).filter(
            (pl.col("date").cast(pl.Utf8) == str(d)) & (pl.col("session") == "Day"))
        return float(df["close"][0]) if df.height else None
    except Exception:  # noqa: BLE001
        return None


def build_forward_curve(spot, futs):
    """由現貨 + 各月台指期建遠期曲線,回 f(T年)->遠期價。
    ln(F) 對 T 分段線性(= 持有成本 carry 逐段累積);超過最遠月沿用末段斜率。"""
    pts = [(0.0, math.log(spot))] + [(T, math.log(F)) for T, F in futs if F > 0]
    pts.sort()

    def fwd(T):
        if T <= 0:
            return spot
        for (t0, l0), (t1, l1) in zip(pts, pts[1:]):
            if t0 <= T <= t1:
                w = (T - t0) / (t1 - t0) if t1 > t0 else 0.0
                return math.exp(l0 + w * (l1 - l0))
        (ta, la), (tb, lb) = pts[-2], pts[-1]
        slope = (lb - la) / (tb - ta) if tb > ta else 0.0
        return math.exp(lb + slope * (T - tb))

    return fwd


def parity_forwards(series, s_ref):
    """逐到期用 put-call parity 反推遠期價:F = K + (C − P)。
    取最接近價平的多檔取中位數(2026-07-24 實測:同到期跨 9 檔全距僅 0~60 點)。
    優點:與 IV 同一批結算價、同一時點,免現貨 13:30 vs 選擇權 13:45 的 stale 落差,
    且不受冷門月份離群成交汙染。年化 carry 異常(>60%)視為壞值丟棄,退回期貨曲線。"""
    byexp = {}
    for s in series:
        byexp.setdefault(s["exp_code"], {}).setdefault(s["K"], {})[s["cp"]] = s
    out = {}
    for code, ks in byexp.items():
        both = {k: v for k, v in ks.items()
                if "C" in v and "P" in v and v["C"]["settle"] and v["P"]["settle"]}
        if len(both) < 3:
            continue
        k0 = min(both, key=lambda k: abs(both[k]["C"]["settle"] - both[k]["P"]["settle"]))
        near = sorted(both, key=lambda k: abs(k - k0))[:9]
        fs = sorted(k + both[k]["C"]["settle"] - both[k]["P"]["settle"] for k in near)
        F, T = fs[len(fs) // 2], both[k0]["C"]["T"]
        # 容忍度隨天期放寬(2%底 + 年化20%);用年化 carry 當閘在超短天期會誤殺:
        # T=2/365 時,0.4% 的正常遠期溢價就等於 73% 年化。
        if F <= 0 or abs(F / s_ref - 1.0) > 0.02 + 0.20 * T:
            continue
        out[code] = F
    return out


def build_series(d):
    """抓當日 TXO 日盤序列,並以「逐到期遠期價」為定價基準。
    回 (series, meta) 或 (None, None)。series 每筆帶 carry,可由任意假設現貨推回遠期。"""
    hdr_o, rows_o = fetch_daily_csv("opt", d)
    hdr_f, rows_f = fetch_daily_csv("fut", d)
    if not rows_o or not rows_f:
        return None, None

    def col(hdr, key):
        for i, c in enumerate(hdr):
            if key in c:
                return i
        return -1

    fc, fe = col(hdr_f, "契約"), col(hdr_f, "到期月份")
    fcl, fs = col(hdr_f, "收盤價"), col(hdr_f, "交易時段")
    fst = col(hdr_f, "結算價")
    # 期貨價一律優先用「結算價」:收盤價在冷門月份是陳舊/離群成交
    # (2026-07-24 實例:202612 收盤 45,950 vs 結算 44,852,差 1,098 點,量僅 38 口)
    futs = sorted((r[fe].strip(), num(r[fst]) or num(r[fcl])) for r in rows_f
                  if r[fc].strip() == "TX" and "一般" in r[fs]
                  and "W" not in r[fe] and "/" not in r[fe]
                  and (num(r[fst]) or num(r[fcl])))
    if not futs:
        return None, None
    fut_front = futs[0][1]

    # A1:標的是 TAIEX,不是期貨。取現貨當錨,各月期貨當遠期曲線的節點。
    spot = taiex_close(d)
    spot_src = "TAIEX(lake)"
    if not spot:                       # 湖裡沒有現貨 → 退回舊行為並在報告標示
        spot, spot_src = fut_front, "TXF近月(現貨缺,降級)"
    fut_pts = []
    for code, px in futs:
        e = expiry_of(code)
        if e and (e - d).days > 0:
            fut_pts.append(((e - d).days / 365.0, px))
    fwd = build_forward_curve(spot, fut_pts)

    oc, oe = col(hdr_o, "契約"), col(hdr_o, "到期月份")
    ok, ocp = col(hdr_o, "履約價"), col(hdr_o, "買賣權")
    ost, ooi = col(hdr_o, "結算價"), col(hdr_o, "未沖銷")
    osess, odue = col(hdr_o, "交易時段"), col(hdr_o, "契約到期日")
    series = []
    for r in rows_o:
        if r[oc].strip() != "TXO" or "一般" not in r[osess]:
            continue
        # 到期日以官方「契約到期日」欄為準(涵蓋 W 週三/F 週五週選);壞值退回代碼推算
        exp = None
        if odue >= 0:
            due = r[odue].strip()
            if len(due) == 8 and due.isdigit():
                exp = date(int(due[:4]), int(due[4:6]), int(due[6:8]))
        if exp is None:
            exp = expiry_of(r[oe])
        if exp is None:
            continue
        Td = (exp - d).days
        if Td <= 0:
            continue
        K, oi, settle = num(r[ok]), num(r[ooi]), num(r[ost])
        if not K or not oi or oi <= 0:
            continue
        series.append({"date": str(d), "exp_code": r[oe].strip().replace(" ", ""),
                       "exp_date": str(exp), "Td": Td, "T": Td / 365.0,
                       "K": K, "cp": "C" if "買" in r[ocp] else "P",
                       "settle": settle, "oi": int(oi), "spot": spot})
    if len(series) < 100:
        return None, None

    # 遠期價來源優先序:① put-call parity(與 IV 同源、免 stale 現貨)② 期貨結算價曲線
    par = parity_forwards(series, spot)
    n_par = 0
    for s in series:
        F = par.get(s["exp_code"]) or fwd(s["T"])
        if s["exp_code"] in par:
            n_par += 1
        s["fwd"] = F
        # 座標一律錨在「近月期貨」:那是你看盤下單的尺,夜盤也有,且不受現貨資料品質影響。
        # 現貨若有誤差,ratio 的分子分母同時受影響會抵銷,不會汙染 flip 位置。
        s["ratio"] = F / fut_front
        s["carry"] = math.log(F / spot) / s["T"] if s["T"] > 0 else 0.0
    # IV:用該到期的遠期價反推(逐 strike,天然含 skew);失敗用該到期中位數補
    by_exp = {}
    for s in series:
        s["iv"] = iv_solve(s["fwd"], s["K"], s["T"], s["settle"], s["cp"])
        if s["iv"]:
            by_exp.setdefault(s["exp_code"], []).append(s["iv"])
    med = {e: sorted(v)[len(v) // 2] for e, v in by_exp.items() if v}
    n_fb = 0
    for s in series:
        if not s["iv"]:
            s["iv"] = med.get(s["exp_code"], 0.2)
            n_fb += 1
    return series, {"S": spot, "spot": spot, "spot_src": spot_src, "fut_front": fut_front,
                    "basis": fut_front - spot, "n_iv_fallback": n_fb, "n_series": len(series),
                    "n_parity": n_par, "n_expiry_parity": len(par)}


def compute_gex(series, S, beta=1.0):
    """GEX(億/1%)、VEX(百萬/vol點)、GEX+(億/1%,含 vanna×spot-vol β 修正)。
    GEX+ 假設:現貨 +1% 時 IV 下跌 beta 個 vol 點(台指典型負相關),
    dealer 每 1% 的避險量 = gamma 腿 + vanna 腿 —— 即羊叔面板的 GEX+ 曲線。"""
    def legs(px, s):
        # 座標=近月期貨價 px;各到期遠期依 parity 求得的比例同步縮放
        F = px * s.get("ratio", math.exp(s.get("carry", 0.0) * s["T"]))
        sq = s["iv"] * math.sqrt(s["T"])
        d1 = (math.log(F / s["K"]) + 0.5 * s["iv"] ** 2 * s["T"]) / sq
        d2 = d1 - sq
        g = npdf(d1) / (F * sq) * s["oi"] * MULT * F * F * 0.01 / 1e8      # 億/1%
        v = F * npdf(d1) * math.sqrt(s["T"]) / 100.0 * s["oi"] * MULT / 1e8  # 億/vol點
        vn = (-npdf(d1) * d2 / s["iv"] / 100.0) * (-beta) * s["oi"] * MULT * F / 1e8  # 億/1%
        return g, v, vn, d1, d2, F

    gex_us, gex_tw, vex_us, vex_tw, gp_us, vex_sh, vex_vn = {}, {}, {}, {}, {}, {}, {}
    wsum = rsum = 0.0
    for s in series:
        g, v, vn, d1_, d2_, F_ = legs(S, s)
        wsum += abs(g)                       # gamma 加權的有效遠期比例:履約價↔期貨的換算基準
        rsum += abs(g) * s.get("ratio", 1.0)
        sgn = 1.0 if s["cp"] == "C" else -1.0
        gex_us[s["K"]] = gex_us.get(s["K"], 0.0) + sgn * g
        gex_tw[s["K"]] = gex_tw.get(s["K"], 0.0) + g
        vex_us[s["K"]] = vex_us.get(s["K"], 0.0) + sgn * v
        vex_tw[s["K"]] = vex_tw.get(s["K"], 0.0) + v
        # 羊叔慣例:假設 dealer 淨賣所有選擇權 → 各履約價一律短 vega(全負)
        vex_sh[s["K"]] = vex_sh.get(s["K"], 0.0) - v
        # 逆向工程自羊叔面板:VEX = −Σ[vanna × OI × 50 × F × (C+/P−)]
        #   vanna=∂Δ/∂σ=−n(d1)·d2/σ(per 1 vol 點);負號=「避險流方向」非「曝險本身」
        #   2026-07-09 驗證:總 −8.99 億(他 −9)、最大 0.389@47000(他 0.39@~47000)、99% 負
        vex_vn[s["K"]] = vex_vn.get(s["K"], 0.0) - sgn * (
            -npdf(d1_) * d2_ / s["iv"] / 100.0) * s["oi"] * MULT * F_ / 1e8
        gp_us[s["K"]] = gp_us.get(s["K"], 0.0) + sgn * (g + vn)
    lo_s, hi_s = int(S * 0.94), int(S * 1.06)
    prof, vanna_leg = [], []
    for Sh in range(lo_s, hi_s, 50):
        tu = tw = tp = tv1 = 0.0
        for s in series:
            g, v, vn, d1u, d2u, Fu = legs(float(Sh), s)
            sgn = 1.0 if s["cp"] == "C" else -1.0
            tu += sgn * g
            tw += g
            tp += sgn * (g + vn)
            # vanna 腿的「每單位 β」值 → 供 β 敏感度掃描(vn 已含 −beta,故除回來)
            tv1 += sgn * (-npdf(d1u) * d2u / s["iv"] / 100.0) * s["oi"] * MULT * Fu / 1e8
        prof.append((Sh, tu, tw, tp))
        vanna_leg.append(tv1)

    def zero_cross(idx):
        for pa, pb in zip(prof, prof[1:]):
            ua, ub = pa[idx], pb[idx]
            if (ua < 0 <= ub) or (ua > 0 >= ub):
                return round(pa[0] + (pb[0] - pa[0]) * (0 - ua) / (ub - ua))
        return None

    # 毛 gamma 峰值:符號無關的主讀值。
    #   gamma 對 Call 與 Put 皆為正 → prof 的第 2 欄(原標「台灣證據版」)Σg 恆正,
    #   數學上就是 Σ|Γ|×OI = **毛 gamma**,不含任何「誰持有」的假設。
    #   它沒有零交叉(恆正)→ 給的是「避險敏感度最集中的價位」(峰),不是 flip。
    gross_peak = max(prof, key=lambda p: p[2])[0] if prof else None
    gross_tot = sum(gex_tw.values())

    # ── β 敏感度:GEX+ Flip 隨 β 的移動範圍 ────────────────────────────
    # 外部專業者(gooptions.cc 2026-07-11)自承 β 是 GEX+ 最弱的假設,但固定 β=1.0。
    # 我們把它掃開:若 flip 隨 β 大幅移動,今天的 GEX+ 讀數就是被假設決定的,不該讀。
    def _cross(vals):
        for (pa, va), (pb, vb) in zip(zip([p[0] for p in prof], vals),
                                      zip([p[0] for p in prof][1:], vals[1:])):
            if (va < 0 <= vb) or (va > 0 >= vb):
                return round(pa + (pb - pa) * (0 - va) / (vb - va))
        return None

    beta_scan = {}
    for bb in (0.5, 1.0, 1.5, 2.0):
        beta_scan[bb] = _cross([p[1] + vl * (-bb) for p, vl in zip(prof, vanna_leg)])
    _bv = [v for v in beta_scan.values() if v]
    beta_span = (max(_bv) - min(_bv)) if len(_bv) >= 2 else None

    # ── VEX 最深履約價(vanna 去穩定最集中處)────────────────────────
    vex_deep = min(vex_vn.items(), key=lambda kv: kv[1]) if vex_vn else (None, 0.0)

    return {"strikes_us": gex_us, "strikes_tw": gex_tw, "vex_us": vex_us, "vex_tw": vex_tw,
            "gp_us": gp_us, "vex_sh": vex_sh, "tot_vex_sh": sum(vex_sh.values()),
            "vex_vn": vex_vn, "tot_vex_vn": sum(vex_vn.values()),
            "gross_peak": gross_peak, "gross_tot": gross_tot,
            "beta_scan": beta_scan, "beta_span": beta_span,
            "vex_deep_k": vex_deep[0], "vex_deep_v": vex_deep[1],
            "profile": prof, "flip_us": zero_cross(1), "flip_gp": zero_cross(3),
            "ratio_eff": (rsum / wsum) if wsum else 1.0,
            "ratio_range": (min((x.get("ratio", 1.0) for x in series), default=1.0),
                            max((x.get("ratio", 1.0) for x in series), default=1.0)),
            "tot_us": sum(gex_us.values()), "tot_tw": sum(gex_tw.values()),
            "tot_vex_us": sum(vex_us.values()), "tot_vex_tw": sum(vex_tw.values()),
            "tot_gp_us": sum(gp_us.values()), "beta": beta,
            "top_pos": sorted(gex_us.items(), key=lambda kv: -kv[1])[:5],
            "top_neg": sorted(gex_us.items(), key=lambda kv: kv[1])[:5]}


# ---------------- 落地 ----------------

def store_quotes(d, series, force=False):
    out = TXO_ROOT / "quotes" / f"{d.year}" / f"TXO_quotes_{d.strftime('%Y%m%d')}.parquet"
    if out.exists() and not force:
        return out, False
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(series).write_parquet(out)
    return out, True


def store_institutional(d, rows):
    if not rows:
        return None
    out = TXO_ROOT / "institutional" / f"pc_{d.year}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    if out.exists():
        df = pl.concat([pl.read_parquet(out), df]).unique(
            subset=["date", "actor", "cp"], keep="last").sort(["date", "actor", "cp"])
    df.write_parquet(out)
    return out


# ---------------- HTML 報告 ----------------

def _svg_bars(gex_us, S, flip, w=880, h=300, thr=0.4, unit="億/1%", conv=1.0):
    """conv:履約價→顯示座標的乘數(TXF座標時 = 1/ratio_eff)。tooltip 保留原始履約價。"""
    raw = dict(gex_us)
    gex_us = {k * conv: v for k, v in gex_us.items()}
    ks = sorted(k for k, v in gex_us.items() if abs(v) > thr and S * 0.94 < k < S * 1.06)
    if not ks:
        return "<p>(無顯著 strike)</p>"
    vmax = max(abs(gex_us[k]) for k in ks) or 1
    x0, x1 = min(ks) - 100, max(ks) + 100
    X = lambda k: 56 + (k - x0) / (x1 - x0) * (w - 76)
    Y = lambda v: h / 2 - v / vmax * (h / 2 - 30)
    parts = [f'<line x1="56" y1="{h/2}" x2="{w-20}" y2="{h/2}" stroke="#2a313c"/>']
    for frac in (1.0, 0.5, -0.5, -1.0):
        gv = vmax * frac
        parts.append(f'<line x1="56" y1="{Y(gv):.0f}" x2="{w-20}" y2="{Y(gv):.0f}" stroke="#1c222b" stroke-dasharray="2 4"/>'
                     f'<text x="50" y="{Y(gv)+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">{gv:+.1f}</text>')
    parts.append(f'<text x="50" y="{h/2+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">0</text>')
    top = sorted(ks, key=lambda k: -abs(gex_us[k]))[:6]
    for k in ks:
        v = gex_us[k]
        c = "#26a69a" if v >= 0 else "#ef5350"
        y, y0 = (Y(v), h / 2) if v >= 0 else (h / 2, Y(v))
        parts.append(f'<rect x="{X(k)-3.5:.0f}" y="{min(y,y0):.0f}" width="7" height="{max(abs(y0-y),2):.0f}" fill="{c}">'
                     f'<title>TXF {k:,.0f}(履約價 {k/conv:,.0f}): {v:+.2f} {unit}</title></rect>')
        if k in top:
            stag = 10 if sorted(top).index(k) % 2 else 0  # 相鄰標籤高低交錯防重疊
            ty = (Y(v) - 5 - stag) if v >= 0 else (Y(v) + 14 + stag)
            parts.append(f'<text x="{X(k):.0f}" y="{ty:.0f}" fill="{c}" font-size="11" text-anchor="middle">{v:+.1f}</text>')
    parts.append(f'<line x1="{X(S):.0f}" y1="20" x2="{X(S):.0f}" y2="{h-20}" stroke="#9aa3ad" stroke-dasharray="5 4"/>'
                 f'<text x="{X(S)+4:.0f}" y="16" fill="#9aa3ad" font-size="15">現價 {S:.0f}</text>')
    if flip and x0 < flip < x1:
        parts.append(f'<line x1="{X(flip):.0f}" y1="20" x2="{X(flip):.0f}" y2="{h-20}" stroke="#f5d90a" stroke-dasharray="3 3"/>'
                     f'<text x="{X(flip)+4:.0f}" y="{h-6}" fill="#f5d90a" font-size="15">flip {flip}</text>')
    for k in range(int(x0 // 500 * 500 + 500), int(x1), 500):
        parts.append(f'<text x="{X(k):.0f}" y="{h-4}" fill="#5a6470" font-size="13" text-anchor="middle">{k}</text>')
    return f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{"".join(parts)}</svg>'


def _svg_curve(profile, S, flip, flip_gp=None, w=880, h=300, only=None):
    xs = [p[0] for p in profile]
    ys = [p[1] for p in profile] + [p[2] for p in profile] + [p[3] for p in profile]
    ymin, ymax = min(ys + [0]), max(ys)
    x0, x1 = xs[0], xs[-1]
    X = lambda x: 50 + (x - x0) / (x1 - x0) * (w - 70)
    Y = lambda v: 20 + (ymax - v) / (ymax - ymin or 1) * (h - 60)
    line = lambda idx, color, dash: (f'<polyline fill="none" stroke="{color}" stroke-width="2.2" '
                                     f'{"stroke-dasharray=\"6 4\"" if dash else ""} points="'
                                     + " ".join(f"{X(p[0]):.0f},{Y(p[idx]):.0f}" for p in profile) + '"/>')
    raw = (ymax - ymin) / 4 or 1
    mag = 10 ** math.floor(math.log10(raw))
    m = raw / mag
    step = mag * (1 if m < 1.5 else 2 if m < 3 else 5 if m < 7 else 10)
    parts = [f'<line x1="50" y1="{Y(0):.0f}" x2="{w-20}" y2="{Y(0):.0f}" stroke="#2a313c"/>',
             f'<text x="44" y="{Y(0)+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">0</text>']
    t = math.ceil(ymin / step) * step
    while t <= ymax:
        if abs(t) > step / 2:
            parts.append(f'<line x1="50" y1="{Y(t):.0f}" x2="{w-20}" y2="{Y(t):.0f}" stroke="#1c222b" stroke-dasharray="2 4"/>'
                         f'<text x="44" y="{Y(t)+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">{t:+,.0f}</text>')
        t += step
    if only == 1:
        parts += [line(1, "#26a69a", False)]
    elif only == 3:
        parts += [line(3, "#b3a4ff", False)]
    else:
        parts += [line(1, "#5e9bd0", False), line(2, "#b3a4ff", True), line(3, "#f0997b", False)]
    parts += [
              f'<line x1="{X(S):.0f}" y1="14" x2="{X(S):.0f}" y2="{h-20}" stroke="#9aa3ad" stroke-dasharray="5 4"/>']
    if flip:
        parts.append(f'<line x1="{X(flip):.0f}" y1="14" x2="{X(flip):.0f}" y2="{h-20}" stroke="#f5d90a" stroke-dasharray="3 3"/>'
                     f'<circle cx="{X(flip):.0f}" cy="{Y(0):.0f}" r="5" fill="#f5d90a"/>')
    if flip_gp and x0 < flip_gp < x1:
        parts.append(f'<circle cx="{X(flip_gp):.0f}" cy="{Y(0):.0f}" r="5" fill="none" stroke="#f0997b" stroke-width="2"/>')
    for k in range(int(x0 // 1000 * 1000 + 1000), int(x1), 1000):
        parts.append(f'<text x="{X(k):.0f}" y="{h-4}" fill="#5a6470" font-size="13" text-anchor="middle">{k}</text>')
    if only is None:
        parts.append(f'<text x="56" y="30" fill="#5e9bd0" font-size="15">GEX 美股慣例(C+/P-) — 符號依賴</text>'
                     f'<text x="56" y="46" fill="#b3a4ff" font-size="15">毛 gamma  &#931;|&#915;|xOI — 符號無關</text>'
                     f'<text x="56" y="62" fill="#f0997b" font-size="15">GEX+(vanna beta 修正) — 符號依賴</text>')
    return f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{"".join(parts)}</svg>'


def clusters(gex, S):
    """上方牆 / 下方地雷:最強點 ±1000 內、強度達 40% 的鄰居併成一片。
    各回 (lo, hi, Σ強度, 加權中心) 或 None。"""
    def one(cands, is_wall):
        if not cands:
            return None
        ka, va = (max if is_wall else min)(cands, key=lambda kv: kv[1])
        mem = [(k, v) for k, v in cands
               if abs(k - ka) <= 1000 and (v >= 0.4 * va if is_wall else v <= 0.4 * va)]
        vs = sum(v for _, v in mem)
        return (min(k for k, _ in mem), max(k for k, _ in mem), vs,
                sum(k * v for k, v in mem) / vs)

    return (one([(k, v) for k, v in gex["strikes_us"].items() if k > S and v > 0.5], True),
            one([(k, v) for k, v in gex["strikes_us"].items() if k < S and v < -0.5], False))


def store_summary(d, meta, gex):
    """每日一列摘要 → 供歷史分位數(B2)與圖vs盤對賬(B3)使用。"""
    rr = gex.get("ratio_eff", 1.0) or 1.0
    wall, mine = clusters(gex, meta["fut_front"] * rr)
    row = {"date": str(d), "spot": meta["spot"], "fut_front": meta["fut_front"],
           "basis": meta["basis"], "ratio_eff": rr, "atr": meta.get("atr"),
           # 語意:**現價相對 flip**(正=現價在 flip 之上)。2026-07-25 由「flip 相對現價」翻轉並改名,
           # 舊欄位 flip_pct/flip_atr 符號相反,勿混用。
           "px_vs_flip_pct": ((meta["fut_front"] / gex["flip_us"] - 1) * 100) if gex["flip_us"] else None,
           "px_vs_flip_atr": ((meta["fut_front"] - gex["flip_us"]) / meta["atr"])
                             if (gex["flip_us"] and meta.get("atr")) else None,
           "flip_us": float(gex["flip_us"]) if gex["flip_us"] else None,
           "flip_gp": float(gex["flip_gp"]) if gex["flip_gp"] else None,
           "tot_us": gex["tot_us"], "tot_tw": gex["tot_tw"],
           "tot_vex_us": gex["tot_vex_us"], "tot_vex_tw": gex["tot_vex_tw"],
           "tot_gp_us": gex["tot_gp_us"],
           "wall_lo": wall[0] if wall else None, "wall_hi": wall[1] if wall else None,
           "wall_val": wall[2] if wall else None,
           "mine_lo": mine[0] if mine else None, "mine_hi": mine[1] if mine else None,
           "mine_val": mine[2] if mine else None}
    p = TXO_ROOT / "daily_summary.parquet"
    df = pl.DataFrame([row])
    if p.exists():
        df = pl.concat([pl.read_parquet(p), df], how="diagonal").unique(
            subset=["date"], keep="last").sort("date")
    df.write_parquet(p)
    return row


def percentiles(d, gex, lookback=60):
    """今日場強在近 N 日的百分位(B2)——沒有歷史座標,厚薄兩字沒有意義。"""
    p = TXO_ROOT / "daily_summary.parquet"
    if not p.exists():
        return {}
    df = pl.read_parquet(p).filter(pl.col("date") <= str(d)).sort("date").tail(lookback)
    out = {"n": df.height}
    for key, col in (("us", "tot_us"), ("tw", "tot_tw"), ("vex", "tot_vex_us")):
        vals = [v for v in df[col].to_list() if v is not None]
        if len(vals) >= 5:
            cur = gex["tot_us"] if key == "us" else (
                gex["tot_tw"] if key == "tw" else gex["tot_vex_us"])
            out[key] = round(sum(1 for v in vals if v <= cur) / len(vals) * 100)
    return out


def atr_txf(d, n=14):
    """TXF 交易日 ATR(日盤+當晚夜盤 合併為一根)—— 把 flip 距離換算成「幾個波動單位」。
    絕對點數在不同價格水準/波動體制間不可比,除以 ATR 才有跨日意義。"""
    p = DATA_ROOT / "kbars" / "1d" / "TXF" / f"TXF_1d_{d.year}.parquet"
    if not p.exists():
        return None
    try:
        df = pl.read_parquet(p).filter(pl.col("date").cast(pl.Utf8) <= str(d))
    except Exception:  # noqa: BLE001
        return None
    g = (df.group_by("date").agg(pl.col("high").max().alias("h"), pl.col("low").min().alias("l"),
                                 pl.col("close").filter(pl.col("session") == "Day").last().alias("c"))
         .drop_nulls().sort("date").tail(n + 1))
    if g.height < 5:
        return None
    rows, trs = g.to_dicts(), []
    for prev, cur in zip(rows, rows[1:]):
        trs.append(max(cur["h"] - cur["l"], abs(cur["h"] - prev["c"]), abs(cur["l"] - prev["c"])))
    return sum(trs) / len(trs) if trs else None


def map_window_bars(m_date, eval_date):
    """地圖(m_date 收盤產出)的正確適用視窗 = m_date 夜盤 + eval_date 日盤。
    ⚠ 湖的夜盤以「起始日」標記(date=7/23 Night 的 ts 是 7/23 15:00 → 7/24 04:55),
    所以視窗要跨兩個 date 標籤取,不能用單一 date 的 Day+Night(那會漏掉前一晚、多算後一晚)。"""
    def rows(dt, sess):
        p = DATA_ROOT / "kbars" / "1d" / "TXF" / f"TXF_1d_{dt.year}.parquet"
        if not p.exists():
            return None
        try:
            df = pl.read_parquet(p).filter(
                (pl.col("date").cast(pl.Utf8) == str(dt)) & (pl.col("session") == sess))
        except Exception:  # noqa: BLE001
            return None
        return df.to_dicts()[0] if df.height else None

    night, day = rows(m_date, "Night"), rows(eval_date, "Day")
    if not day:
        return None
    parts = [x for x in (night, day) if x]
    return {"open": parts[0]["open"], "close": day["close"],
            "high": max(x["high"] for x in parts), "low": min(x["low"] for x in parts),
            "has_night": night is not None}


def reconcile(d):
    """B3:拿「前一交易日的地圖」對照「今天實際走勢」,逐日累積成 Phase 1 資料集。"""
    p = TXO_ROOT / "daily_summary.parquet"
    if not p.exists():
        return None
    hist = pl.read_parquet(p).filter(pl.col("date") < str(d)).sort("date")
    if not hist.height:
        return None
    m = hist.tail(1).to_dicts()[0]
    bars = map_window_bars(datetime.strptime(m["date"], "%Y-%m-%d").date(), d)
    if not bars or not m.get("flip_us"):
        return None
    flip_f = m["flip_us"]                # flip 已是 TXF 座標(近月期貨軸)
    basis = 0.0
    above = m["fut_front"] > flip_f
    broke = (bars["low"] < flip_f) if above else (bars["high"] > flip_f)
    rng = bars["high"] - bars["low"]
    row = {"map_date": m["date"], "eval_date": str(d),
           "tot_us": m["tot_us"], "tot_tw": m["tot_tw"],
           "flip_fut": flip_f, "above_flip": above, "broke_flip": broke,
           "open": bars["open"], "close": bars["close"],
           "high": bars["high"], "low": bars["low"],
           "move": bars["close"] - bars["open"], "range": rng,
           "has_night": bars.get("has_night"),
           "range_pct": rng / bars["open"] * 100,
           "hit_wall": (bars["high"] >= m["wall_lo"] / (m.get("ratio_eff") or 1.0))
                       if m.get("wall_lo") else None,
           "hit_mine": (bars["low"] <= m["mine_hi"] / (m.get("ratio_eff") or 1.0))
                       if m.get("mine_hi") else None}
    rp = TXO_ROOT / "reconcile.parquet"
    df = pl.DataFrame([row])
    if rp.exists():
        df = pl.concat([pl.read_parquet(rp), df], how="diagonal").unique(
            subset=["map_date"], keep="last").sort("map_date")
    df.write_parquet(rp)
    print(f"[RECON] 地圖{m['date']} → {d}:{'破' if broke else '守'}flip"
          f"{flip_f:,.0f} 幅度{rng:.0f}點({row['range_pct']:.2f}%)")
    return row


def _plain_map(gex, fut_front, atr=None, w=880, h=250):
    """白話版地圖 —— 一律 TXF 座標(你看盤下單的尺、夜盤也有、免受現貨資料品質影響)。
    flip 本來就在 TXF 軸上;履約價則以 gamma 加權有效遠期比例換算,原始履約價留在 tooltip。"""
    rr = gex.get("ratio_eff", 1.0) or 1.0
    conv = lambda k: k / rr                  # 履約價 → TXF 等價位
    S = fut_front
    wall_r, mine_r = clusters(gex, S * rr)   # 牆/地雷的搜尋在履約價座標進行
    fa, fb = gex["flip_us"], gex["flip_gp"]
    dist_lbl = "基本版:在上=偏穩"
    if fa:
        _p = (fut_front / fa - 1) * 100          # 現價相對 flip(正=現價在上)
        dist_lbl = (f"現價在其{'上方' if _p > 0 else '下方'} {abs(_p):.2f}%"
                    + (f" · {abs(fut_front-fa)/atr:.2f} ATR" if atr else ""))
    wall = (conv(wall_r[0]), conv(wall_r[1]), wall_r[2], conv(wall_r[3]), wall_r[0], wall_r[1]) if wall_r else None
    mine = (conv(mine_r[0]), conv(mine_r[1]), mine_r[2], conv(mine_r[3]), mine_r[0], mine_r[1]) if mine_r else None
    pts = [S] + [x for x in (fa, fb) if x]
    for c in (wall, mine):
        if c:
            pts += [c[0], c[1]]
    lo, hi = min(pts) - S * 0.008, max(pts) + S * 0.008
    if hi - lo < S * 0.02:
        pad = (S * 0.02 - (hi - lo)) / 2
        lo, hi = lo - pad, hi + pad
    X = lambda x: 60 + (x - lo) / (hi - lo) * (w - 120)
    P = []
    P.append(f'<line x1="60" y1="150" x2="{w-60}" y2="150" stroke="#888780" stroke-width="2"/>')
    P.append(f'<text x="60" y="174" fill="#5a6470" font-size="13">{lo:,.0f}</text>')
    P.append(f'<text x="{w-60}" y="174" fill="#5a6470" font-size="13" text-anchor="end">{hi:,.0f}</text>')
    if mine:
        lo_k, hi_k, vs, ctr, rlo, rhi = mine
        lab = f"{lo_k:,.0f}" if lo_k == hi_k else f"{lo_k:,.0f}~{hi_k:,.0f}"
        raw = f"履約價 {rlo:,.0f}" if rlo == rhi else f"履約價 {rlo:,.0f}~{rhi:,.0f}"
        big = "大" if (abs(vs) >= 4 and abs(vs) >= 0.15 * gex["tot_tw"]) else "小"
        if lo_k != hi_k:
            P.append(f'<line x1="{X(lo_k):.0f}" y1="150" x2="{X(hi_k):.0f}" y2="150" stroke="#ef5350" stroke-width="7" opacity="0.3"/>')
        P.append(f'<line x1="{X(ctr):.0f}" y1="150" x2="{X(ctr):.0f}" y2="118" stroke="#ef5350" stroke-width="2"/>'
                 f'<circle cx="{X(ctr):.0f}" cy="110" r="7" fill="#2a1416" stroke="#ef5350" stroke-width="2">'
                 f'<title>{raw} · 合計 {vs:.1f} 億/1%</title></circle>'
                 f'<text x="{X(ctr):.0f}" y="92" fill="#ef5350" font-size="14" text-anchor="middle">{lab} {big}地雷</text>'
                 f'<text x="{X(ctr):.0f}" y="76" fill="#9aa3ad" font-size="12" text-anchor="middle">跌破可能越跌越順(合計{vs:.1f})</text>')
    if wall:
        lo_k, hi_k, vs, ctr, rlo, rhi = wall
        lab = f"{lo_k:,.0f}" if lo_k == hi_k else f"{lo_k:,.0f}~{hi_k:,.0f}"
        raw = f"履約價 {rlo:,.0f}" if rlo == rhi else f"履約價 {rlo:,.0f}~{rhi:,.0f}"
        big = "大" if (vs >= 6 and vs >= 0.15 * gex["tot_tw"]) else "小"
        if lo_k != hi_k:
            P.append(f'<line x1="{X(lo_k):.0f}" y1="150" x2="{X(hi_k):.0f}" y2="150" stroke="#26a69a" stroke-width="7" opacity="0.3"/>')
        P.append(f'<line x1="{X(ctr):.0f}" y1="150" x2="{X(ctr):.0f}" y2="118" stroke="#26a69a" stroke-width="2"/>'
                 f'<rect x="{X(ctr)-30:.0f}" y="98" width="60" height="20" rx="4" fill="#12261f" stroke="#26a69a">'
                 f'<title>{raw} · 合計 +{vs:.1f} 億/1%</title></rect>'
                 f'<text x="{X(ctr):.0f}" y="112" fill="#26a69a" font-size="13" text-anchor="middle">{big}牆</text>'
                 f'<text x="{X(ctr):.0f}" y="86" fill="#9aa3ad" font-size="12" text-anchor="middle">{lab} 漲到易黏(合計+{vs:.1f})</text>')
    if fa:
        P.append(f'<line x1="{X(fa):.0f}" y1="150" x2="{X(fa):.0f}" y2="196" stroke="#f5d90a" stroke-width="2" stroke-dasharray="5 3"/>'
                 f'<text x="{X(fa):.0f}" y="216" fill="#f5d90a" font-size="13" text-anchor="middle">參考線A {fa:,.0f}</text>'
                 f'<text x="{X(fa):.0f}" y="232" fill="#9aa3ad" font-size="12" text-anchor="middle">{dist_lbl}</text>')
    if fb:
        P.append(f'<line x1="{X(fb):.0f}" y1="150" x2="{X(fb):.0f}" y2="180" stroke="#f0997b" stroke-width="2" stroke-dasharray="5 3"/>'
                 f'<text x="{X(fb):.0f}" y="198" fill="#f0997b" font-size="13" text-anchor="middle">參考線B {fb:,.0f}(進階版)</text>')
    P.append(f'<circle cx="{X(S):.0f}" cy="150" r="9" fill="#f5d90a" stroke="#0e1116" stroke-width="2"/>'
             f'<text x="{X(S):.0f}" y="128" fill="#f5d90a" font-size="15" text-anchor="middle">TXF 收盤 {S:,.0f}</text>')
    tot = gex["tot_us"]
    lvl = "強" if abs(tot) >= 40 else ("中等" if abs(tot) >= 10 else "弱")
    P.append(f'<rect x="60" y="14" width="560" height="26" rx="6" fill="#161b22" stroke="#2a313c"/>'
             f'<text x="72" y="32" fill="#9aa3ad" font-size="13">今日整體力量:{lvl}(總GEX {tot:+.1f} 億/1%)'
             f' · 本圖為 TXF 座標(履約價 ×{1/rr:.4f})</text>')
    svg = f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{"".join(P)}</svg>'

    if fa and fb:
        if S > max(fa, fb):
            stance = "現價站在兩條參考線之上:偏「彈簧」日(莊家避險傾向把價格拉回,盤面偏穩、愛盤整)"
        elif S < min(fa, fb):
            stance = "現價跌在兩條參考線之下:偏「雪球」日(避險會推著價格走,盤面偏晃、容易走出趨勢)"
        else:
            stance = "現價被兩條參考線一上一下夾住:兩台儀器意見相左=曖昧日,別靠這張圖選方向"
    elif fa or fb:
        f0 = fa or fb
        stance = ("現價在參考線之上:偏彈簧(穩)" if S > f0 else "現價在參考線之下:偏雪球(晃)")
    else:
        stance = "今天沒有翻轉線(整條曲線同號):全域偏單一體制"
    sent = f"{stance}。今日力量{lvl}" + ("——就算有牆有地雷,威力也都是縮小版,別過度解讀。" if lvl == "弱" else "。")
    return svg, sent


def _svg_signlog(d, days=45, w=880, h=230):
    """B4:自營商 call/put 淨部位時間序列 —— 「符號會不會翻面」的證據鏈。"""
    p = TXO_ROOT / "institutional" / f"pc_{d.year}.parquet"
    if not p.exists():
        return "<p class='mut'>(尚無符號日誌)</p>"
    df = (pl.read_parquet(p).filter(pl.col("actor") == "自營商")
          .with_columns((pl.col("long_oi") - pl.col("short_oi")).alias("net"))
          .pivot(values="net", index="date", on="cp").sort("date").tail(days))
    if df.height < 3 or "C" not in df.columns or "P" not in df.columns:
        return "<p class='mut'>(符號日誌樣本不足)</p>"
    rows = df.to_dicts()
    vals = [v for r in rows for v in (r.get("C"), r.get("P")) if v is not None]
    ymin, ymax = min(vals + [0]), max(vals + [0])
    pad = (ymax - ymin) * 0.12 or 1
    ymin, ymax = ymin - pad, ymax + pad
    X = lambda i: 66 + i / max(len(rows) - 1, 1) * (w - 96)
    Y = lambda v: 18 + (ymax - v) / (ymax - ymin) * (h - 56)
    P = [f'<line x1="66" y1="{Y(0):.0f}" x2="{w-30}" y2="{Y(0):.0f}" stroke="#3a424e"/>'
         f'<text x="60" y="{Y(0)+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">0</text>']
    for frac in (0.6,):
        for v in (ymax * frac, ymin * frac):
            if abs(v) > 500:
                P.append(f'<line x1="66" y1="{Y(v):.0f}" x2="{w-30}" y2="{Y(v):.0f}" stroke="#1c222b" stroke-dasharray="2 4"/>'
                         f'<text x="60" y="{Y(v)+5:.0f}" fill="#5a6470" font-size="12" text-anchor="end">{v:+,.0f}</text>')
    for key, color in (("C", "#26a69a"), ("P", "#f5d90a")):
        pts = " ".join(f"{X(i):.0f},{Y(r[key]):.0f}" for i, r in enumerate(rows) if r.get(key) is not None)
        P.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.2" points="{pts}"/>')
        last = rows[-1].get(key)
        if last is not None:
            P.append(f'<circle cx="{X(len(rows)-1):.0f}" cy="{Y(last):.0f}" r="4" fill="{color}"/>')
    neg = sum(1 for r in rows if (r.get("P") or 0) < 0)
    P.append(f'<text x="70" y="{h-8}" fill="#26a69a" font-size="14">— CALL 淨部位</text>'
             f'<text x="210" y="{h-8}" fill="#f5d90a" font-size="14">— PUT 淨部位</text>'
             f'<text x="360" y="{h-8}" fill="#9aa3ad" font-size="14">正=淨買方(long gamma 側);'
             f'近{len(rows)}日 PUT 翻負 {neg} 天</text>')
    P.append(f'<text x="66" y="14" fill="#5a6470" font-size="12">{rows[0]["date"]}</text>'
             f'<text x="{w-30}" y="14" fill="#5a6470" font-size="12" text-anchor="end">{rows[-1]["date"]}</text>')
    return f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{"".join(P)}</svg>'


def _html_recon(d, n=6):
    """B3 面板:近幾日「地圖 vs 實際」對賬 + 依體制分組的已實現波動。"""
    p = TXO_ROOT / "reconcile.parquet"
    if not p.exists():
        return "<p class='mut'>(對賬資料累積中,需至少兩個交易日)</p>"
    df = pl.read_parquet(p).sort("map_date")
    rec = df.tail(n).reverse().to_dicts()
    tr = "".join(
        f"<tr><td>{r['map_date']}→{r['eval_date'][5:]}</td>"
        f"<td>{r['tot_us']:+.1f}</td>"
        f"<td>{'上' if r['above_flip'] else '下'} {r['flip_fut']:,.0f}</td>"
        f"<td>{'✅守住' if not r['broke_flip'] else '❌跌破' if r['above_flip'] else '❌突破'}</td>"
        f"<td>{r['range']:.0f} ({r['range_pct']:.2f}%)</td>"
        f"<td>{r['move']:+.0f}</td></tr>" for r in rec)
    agg = ""
    if df.height >= 6:
        g = (df.group_by("above_flip")
             .agg(pl.len().alias("n"), pl.col("range_pct").mean().alias("avg"))
             .sort("above_flip", descending=True).to_dicts())
        agg = "<p style='margin:10px 0 0'>依體制分組(樣本太少,僅供累積觀察):" + " · ".join(
            f"flip{'之上' if r['above_flip'] else '之下'} n={r['n']} 平均日振幅 {r['avg']:.2f}%"
            for r in g) + "</p>"
    return (f"<table><tr><th>地圖日→適用日</th><th>總GEX</th><th>當時位置</th><th>flip 結果</th>"
            f"<th>實際振幅</th><th>漲跌</th></tr>{tr}</table>{agg}")


def render_html(d, S, meta, gex, inst, expiries, pct=None):
    flip = gex["flip_us"]
    basis = meta.get("basis", 0.0)
    rr = gex.get("ratio_eff", 1.0) or 1.0
    regime = ("現價在 flip 之上 → 美股慣例讀為 +γ 壓抑區" if flip and S > flip else
              "現價在 flip 之下 → 美股慣例讀為 -γ 放大區" if flip else "無 flip(全域同號)")
    pct = pct or {}
    pct_line = ""
    if pct.get("us") is not None:
        pct_line = (f"<div class='panel'>場強座標(近 {pct['n']} 個交易日):總 GEX 在第 "
                    f"<b class='warn'>{pct['us']}</b> 百分位"
                    + (f"、台灣版第 <b>{pct['tw']}</b> 百分位" if pct.get("tw") is not None else "")
                    + (f"、總 VEX 第 <b>{pct['vex']}</b> 百分位" if pct.get("vex") is not None else "")
                    + " —— 低百分位=莊家穩定力量偏弱的日子。</div>")
    pos = " · ".join(f"<b>{k/rr:,.0f}</b><span class='mut'>(履約{k:.0f})</span> +{v:.1f}"
                     for k, v in gex["top_pos"])
    neg = " · ".join(f"<b>{k/rr:,.0f}</b><span class='mut'>(履約{k:.0f})</span> {v:.1f}"
                     for k, v in gex["top_neg"])
    inst_html = "<p class='mut'>三大法人分計:本日抓取失敗(不影響 GEX)。</p>"
    if inst:
        r = {(x["actor"], x["cp"]): x for x in inst}
        def net(a, cp):
            x = r.get((a, cp))
            return f"{x['long_oi']-x['short_oi']:+,}" if x else "?"
        inst_html = (f"<p>自營商淨部位:CALL {net('自營商','C')} 口、PUT {net('自營商','P')} 口 | "
                     f"外資:CALL {net('外資','C')}、PUT {net('外資','P')}(正=淨買方)</p>")
    exp_str = " · ".join(f"{e['code']}({e['days']}d)" for e in expiries[:4])
    plain_svg, plain_sent = _plain_map(gex, meta['fut_front'], meta.get('atr'))
    flip_txf_note = ((f"履約價座標 {flip*rr:,.0f} · GEX+ flip TXF {gex['flip_gp']:,.0f}"
                      if gex['flip_gp'] else f"履約價座標 {flip*rr:,.0f}") if flip else "無翻轉點")
    fut_conv = meta["fut_front"] * (1 - rr)
    atr = meta.get("atr")
    if flip:
        # 語意:**現價相對 flip**(正=現價在 flip 之上=+γ 壓抑區;負=在下=−γ 放大區)
        # 這樣符號直接對應體制,不必再看文字。(2026-07-25 修正:原本算「flip 減現價」,
        #  正負與直覺相反,曾造成「現價低於 flip 卻顯示 +0.28%」的困惑。)
        dp = (meta["fut_front"] / flip - 1) * 100
        da = (meta["fut_front"] - flip) / atr if atr else None
        zone = "上方(+γ 壓抑)" if dp > 0 else "下方(−γ 放大)"
        dist_txt = f"{dp:+.2f}%"
        dist_sub = (f"現價在 flip {zone} · {da:+.2f} ATR({atr:,.0f} 點) · {meta['fut_front']-flip:+,.0f} 點"
                    if da is not None else f"現價在 flip {zone} · {meta['fut_front']-flip:+,.0f} 點")
    else:
        dist_txt, dist_sub = "N/A", "今日無翻轉點"
    flip_disp = f"{flip:,.0f}" if flip else "N/A"
    flip_spot = f"{flip*rr:,.0f}" if flip else "N/A"
    _rs = gex.get("ratio_range") or (rr, rr)
    conv_span = (44000/_rs[0] - 44000/_rs[1]) if _rs[0] != _rs[1] else 0.0
    conv_note = f"履約價 44,000 → TXF {44000/rr:,.0f}"
    gp_spot = f"{gex['flip_gp']*rr:,.0f}" if gex["flip_gp"] else "N/A"
    gp_disp = f"{gex['flip_gp']:,.0f}" if gex["flip_gp"] else "N/A"
    _gpk = gex.get("gross_peak")
    gross_peak_disp = (f"TXF {_gpk:,.0f}(現貨 {_gpk*rr:,.0f})" if _gpk else "N/A")

    # ── β 敏感度:GEX+ Flip 隨 β 移動多少 → 今天的 GEX+ 能不能讀 ──────
    _bs, _span = gex.get("beta_scan") or {}, gex.get("beta_span")
    _atr_v = atr if atr else None
    if _span is None:
        beta_v, beta_cls, beta_sub = "N/A", "mut", "β 掃描無交叉點"
    else:
        _thr = (0.25 * _atr_v) if _atr_v else 150.0
        ok = _span <= _thr
        beta_v = f"{_span:,.0f} 點"
        beta_cls = "pos" if ok else "neg"
        beta_sub = (("✅ 可讀" if ok else "❌ 不可讀,今天的 GEX+ 是被假設決定的") +
                    f" · 位移 {_span:,.0f} 點 · β0.5–2.0 → " +
                    " / ".join(f"{v:,.0f}" if v else "—" for v in _bs.values()) +
                    (f" · 門檻 0.25ATR={_thr:,.0f}" if _atr_v else ""))

    # ── 灰色地帶:Gamma Flip 與 GEX+ Flip 之間 ─────────────────────────
    # vanna 位移 = GEX+ Flip − Gamma Flip。
    # ⚠ 不用「現價在不在兩條 flip 之間」:實測 22/23 天都成立 = 零鑑別力,已棄用。
    #   有資訊的是**位移量**(實測 70–512 點,7 倍差距)——它衡量 vanna 這一層把地圖搬多遠。
    _gf, _pf = flip, gex.get("flip_gp")
    if _gf and _pf:
        _sh = _pf - _gf
        _r = (abs(_sh) / _atr_v) if _atr_v else None
        gray_v = f"{_sh:+,.0f} 點"
        gray_cls = "neg" if (_r is not None and _r >= 0.25) else "mut"
        gray_sub = ((f"= {_r:.2f} ATR · " if _r is not None else "") +
                    ("⚠ vanna 大幅改寫地圖" if (_r is not None and _r >= 0.25)
                     else "vanna 影響有限") +
                    f"<br>Gamma {_gf:,.0f} → GEX+ {_pf:,.0f}")
    else:
        gray_v, gray_cls, gray_sub = "N/A", "mut", "缺一條 flip"

    # ── VEX 最深履約價 ────────────────────────────────────────────────
    _vk, _vv = gex.get("vex_deep_k"), gex.get("vex_deep_v") or 0.0
    if _vk:
        _vtxf = _vk / rr
        _off = (_vtxf / meta.get("fut_front", _vtxf) - 1) * 100
        vex_deep_v = f"TXF {_vtxf:,.0f}"
        vex_deep_sub = (f"{_vv:+.2f} 億/vol點 · 離現價 {_off:+.1f}%"
                        "<br>平常沒感覺;IV 噴發時 vanna 從這裡回來")
    else:
        vex_deep_v, vex_deep_sub = "N/A", ""
    recon_html = _html_recon(d)
    signlog_svg = _svg_signlog(d)
    html = f"""<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8">
<title>TXO GEX {d}</title><style>
body{{margin:0;background:#0e1116;color:#e6e8eb;font-family:"Microsoft JhengHei","Noto Sans TC",sans-serif;line-height:1.8;font-size:17px}}
.wrap{{max-width:1060px;margin:0 auto;padding:24px 18px 60px}} h1{{font-size:27px;margin:0}}
h2{{font-size:21px;margin:28px 0 10px;border-left:4px solid #f5d90a;padding-left:10px}}
.mut{{color:#9aa3ad;font-size:15px}} .cards{{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0}}
.card{{background:#161b22;border:1px solid #2a313c;border-radius:10px;padding:12px 18px;min-width:170px}}
.card .n{{font-size:14px;color:#9aa3ad}} .card .v{{font-size:26px}}
.pos{{color:#26a69a}} .neg{{color:#ef5350}} .warn{{color:#f5d90a}}
svg{{width:100%;height:auto;background:#0b0e13;border:1px solid #2a313c;border-radius:8px}}
.panel{{background:#161b22;border:1px solid #2a313c;border-radius:10px;padding:12px 18px;margin:12px 0;font-size:17px}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:16px 0}}
@media(max-width:900px){{.grid{{grid-template-columns:1fr}}}}
.cell{{background:#0f1620;border:1px solid #2a313c;border-radius:10px;padding:10px 12px}}
.cell h3{{margin:0 0 6px;font-size:16px;color:#e6e8eb;text-align:center}}
.cell .sub{{font-size:13px;color:#9aa3ad;text-align:center;margin:0 0 6px}}
.cell svg{{border:0;background:transparent}}
</style></head><body><div class="wrap">
<h1>TXO 做市商曝險地圖 <span class="mut">{d}(收盤)</span></h1>
<p class="mut">產生 {datetime.now().strftime('%Y-%m-%d %H:%M')} | 序列 {meta['n_series']} 條 | 到期:{exp_str} |
IV 反推失敗補中位:{meta['n_iv_fallback']} 條 | 遠期價來源:put-call parity {meta.get('n_expiry_parity','?')} 個到期
(其餘退回期貨結算價曲線)| GEX+ β={gex['beta']:.1f}</p>
<div class="panel" style="border-color:#3a4553">
<b>讀法:只讀「位置」,不讀「正負」。</b>
逐筆量測 39 個交易日,固定符號慣例約 <b>3/4 的日子是錯的</b>;
牆在價格空間<b>不存在</b>(gamma 卷積寬度是履約價間距的 6–25 倍);
逐日淨流量對隔日<b>無預測力</b>(R²=0.011)。細節見下方與 wiki 墳場 #13。
</div>
<div class="cards">
<div class="card"><div class="n">TXF 近月(結算價)</div><div class="v">{meta.get('fut_front',0):,.0f}</div>
<div class="n">TAIEX 現貨 {S:,.0f}(基差 {basis:+.0f})</div></div>

<div class="card"><div class="n">毛 gamma 峰值 <b>(符號無關)</b></div><div class="v">{gross_peak_disp}</div>
<div class="n">避險敏感度最集中處 · 不含任何「誰持有」假設</div></div>

<div class="card"><div class="n">現價 vs Gamma flip</div><div class="v warn">{dist_txt}</div>
<div class="n">{dist_sub}<br>flip 位置對符號翻轉<b>不變</b>;上下方的壓抑/放大標籤<b>會對調</b></div></div>

<div class="card"><div class="n">GEX+ Flip 與 β 敏感度</div><div class="v {beta_cls}">{gp_disp}</div>
<div class="n">{beta_sub}</div></div>

<div class="card"><div class="n">VEX 最深履約價</div><div class="v">{vex_deep_v}</div>
<div class="n">{vex_deep_sub}</div></div>
</div>
<h2>結構地圖</h2>
{plain_svg}
<div class="panel">{plain_sent}</div>
{pct_line}
<p class="mut">摘要卡與本圖=<b>TXF 座標</b>(可直接對照看盤);下方柱狀圖=<b>履約價座標</b>,換算 ×{1/rr:.4f}(≈{fut_conv:+.0f} 點,由選擇權自身遠期價推得,盤中微幅漂移)。</p>
<h2>地圖 vs 實際盤面(逐日對賬)</h2>
<p class="mut">回填「前一日地圖說什麼 → 當日實際怎麼走」。<b>這是唯一能證明這張圖有沒有用的東西</b>,也是報表裡最該看的一段。</p>
{recon_html}
<h2>逐履約價分布(兩格)</h2>
<div class="grid">
  <div class="cell"><h3>GEX 各履約價 <span class="mut">⚠符號依賴</span></h3>
    <p class="sub">總 {gex['tot_us']:+.1f} 億NT$/1%(美股慣例)· 位置可讀、顏色不可讀</p>
    {_svg_bars(gex['strikes_us'], S, flip, w=700, h=340, conv=1/rr)}</div>
  <div class="cell"><h3>VEX 各履約價 <span class="mut">最深 {vex_deep_v}</span></h3>
    <p class="sub">總 {gex['tot_vex_vn']:+.1f} 億NT$/vol點 · vanna 曝險(非 vega)</p>
    {_svg_bars(gex['vex_vn'], S, None, thr=0.02, unit="億/vol點", w=700, h=340, conv=1/rr)}</div>
</div>
<p class="mut">兩格皆 <b>TXF 座標</b>(履約價 ×{1/rr:.4f},今日 {conv_note};嚴格說是「gamma 主要所在到期」的座標,
各到期換算全距 {conv_span:,.0f} 點,因 gamma 95% 集中近端故不影響看盤)。
<b>只看 gamma 與 vanna 的理由</b>:只有會改變 delta 的 Greek 才會逼 dealer 交易期貨——
delta 的偏導數是 gamma(價格)、vanna(IV)、charm(時間);vega/theta 只影響損益不影響 delta。
公式已與外部專業面板逐項對驗(2026-07-09:VEX 總 −8.99 vs −9 億、Gamma Flip 差 1 點)。
<h2>GEX(S) 曲線 <span class="mut">紫=毛 gamma(符號無關)· 藍/橘=符號依賴</span></h2>{_svg_curve(gex['profile'], S, flip, gex['flip_gp'])}
<p class="mut">紫線是 <b>&Sigma;|&Gamma;|&times;OI</b>(gamma 對 Call/Put 皆正,不帶符號的加總即毛 gamma)——<b>不含任何「誰持有」假設</b>,
恆正無零交叉,給的是「避險敏感度最集中的價位」而非 flip。今日峰值 <b>{gross_peak_disp}</b>(總 {gex['gross_tot']:.1f} 億NT$/1%)。
黃線=Gamma Flip、橘圈=GEX+ Flip,兩者距離即上方的 <b>vanna 位移</b>。</p>

<h2>OI 集中價位 <span class="mut">(事實,不是支撐壓力)</span></h2>
<div class="panel"><span class="pos">正 GEX 集中:</span>{pos}<br><span class="neg">負 GEX 集中:</span>{neg}</div>

<h2>為什麼只讀位置 <span class="mut">(三項實證,點開即懂)</span></h2>
<div class="panel mut">
<table style="border-collapse:collapse;font-size:15px">
<tr><td style="padding:5px 16px 5px 0;white-space:nowrap"><b>① 符號不固定</b></td>
    <td>逐日 (Call淨,Put淨):<b>C&minus;/P+ 43.6%</b> · C+/P&minus;(本圖採用)25.6% · C+/P+ 17.9% · C&minus;/P&minus; 12.8%。
    <b>但 flip 的「位置」對全域符號翻轉不變</b>(C&minus;/P+ = C+/P&minus; 取負,零交叉點相同)&rarr; 位置可讀、
    壓抑/放大的標籤約擲硬幣;同號組態(30.7% 的日子)則<b>根本沒有 flip</b>。</td></tr>
<tr><td style="padding:5px 16px 5px 0;white-space:nowrap"><b>② 牆不存在</b></td>
    <td>669,873 個 1 分鐘 bar / 613 日:逐日 6 次多項式對毛 gamma 曲線 <b>R&sup2;=1.0000</b>、殘差僅 0.1%
    &rarr; 曲線就是距離的平滑函數,沒有離散結構。機制:單一履約價的 gamma 鋪開 &asymp; &sigma;&radic;T&middot;S,
    30 天/IV20% 是 2,523 點、<b>剩 0.5 天仍有 570 點</b>,是履約價間距的 6&ndash;25 倍 &rarr; 完全重疊被抹平。
    另:結算釘價檢定 181 到期/6,030 strike,c=&minus;0.13(t=&minus;0.32)、c&lt;0 僅 49.7%。</td></tr>
<tr><td style="padding:5px 16px 5px 0;white-space:nowrap"><b>③ 不領先</b></td>
    <td>逐日淨流量對<b>隔日</b>報酬:t=+0.59、R&sup2;=0.011。同期 R&sup2;=0.081 &rarr; 純描述,不預測。</td></tr>
</table>
<b>&rarr; 上面長條圖看得到「牆」是因為那畫的是 OI 本身;能影響價格的是 gamma 對現價的卷積,而它是平滑的。</b>
詳見 wiki 墳場 #13。</p>
<h2>符號日誌(自營商淨部位走勢)</h2>
<p class="mut">整張圖的紅綠該不該翻,取決於做市商(自營商為最接近的代理)站買方還賣方。這條線是台灣獨有、美股沒有的直讀證據。</p>
{signlog_svg}
<div class="panel">{inst_html}</div>
<h2>每日 SOP <span class="mut">五個數字,其餘不必看</span></h2><div class="panel">
<table style="border-collapse:collapse;font-size:16px">
<tr><td style="padding:4px 14px 4px 0"><b>1</b></td><td style="padding-right:14px"><b>毛 gamma 峰值</b></td>
    <td>{gross_peak_disp}</td><td class="mut">符號無關,唯一不帶假設的讀值</td></tr>
<tr><td style="padding:4px 14px 4px 0"><b>2</b></td><td><b>現價 vs flip</b></td>
    <td>{dist_txt}</td><td class="mut">±150 點內=無人區,不判讀</td></tr>
<tr><td style="padding:4px 14px 4px 0"><b>3</b></td><td><b>β 敏感度</b></td>
    <td class="{beta_cls}">{beta_v}</td><td class="mut">超過 0.25 ATR &rarr; 今天 GEX+ 不可讀</td></tr>
<tr><td style="padding:4px 14px 4px 0"><b>4</b></td><td><b>VEX 最深履約價</b></td>
    <td>{vex_deep_v}</td><td class="mut">IV 噴發時 vanna 從這裡回來</td></tr>
<tr><td style="padding:4px 14px 4px 0"><b>5</b></td><td><b>距結算</b></td>
    <td>{f"{expiries[0]['days']:.0f}" if expiries else '?'} 天</td>
    <td class="mut">最近到期 {expiries[0]['code'] if expiries else '?'} · 結算後地圖整個重繪</td></tr>
</table>
<hr style="border:0;border-top:1px solid #2a313c;margin:10px 0">
<b class="neg">❌ 已判死,不要這樣用</b><span class="mut">:①flip 當方向濾網決定順勢/均值回歸(&lambda; 差僅 +7%、日盤零效應,扣不動 3&ndash;4 點)
②淨流量預測隔日(R&sup2;=0.011)③牆當支撐壓力(結構不存在)。</span><br>
<b>定位:逐 strike 歷史的累積器 + 結構事實的觀察日誌,不是交易輸入。</b></div>
</div></body></html>"""
    rpt_dir = TXO_ROOT / "reports"
    rpt_dir.mkdir(parents=True, exist_ok=True)
    out = rpt_dir / f"gex_{d.strftime('%Y%m%d')}.html"
    out.write_text(html, encoding="utf-8")
    (rpt_dir / "latest.html").write_text(html, encoding="utf-8")
    return out


def log_event(rec):
    """把每次取得嘗試寫進 JSONL —— 事後可統計資料真正公布的時間分布,用來校準排程。"""
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **rec}
    p = TXO_ROOT / "logs" / "fetch_log.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def backfill_institutional(d, days=7):
    """回補最近 days 個「已有 quotes 卻缺三大法人」的交易日(法人資料晚出時的補救)。"""
    p = TXO_ROOT / "institutional" / f"pc_{d.year}.parquet"
    have = set(pl.read_parquet(p)["date"].to_list()) if p.exists() else set()
    done = []
    for f in sorted(TXO_ROOT.glob("quotes/*/TXO_quotes_*.parquet"))[-days:]:
        fd = datetime.strptime(f.stem[-8:], "%Y%m%d").date()
        if str(fd) in have:
            continue
        rows = fetch_institutional(fd)
        if rows:
            store_institutional(fd, rows)
            done.append(str(fd))
    if done:
        print(f"[INST-BACKFILL] 補齊 {len(done)} 天:{', '.join(done)}")
    return done


def run_with_wait(d, deadline_hhmm="16:30", poll_sec=180, force=False):
    """輪詢等待資料公布:一拿到就出圖收工;逾時放棄並記錄(假日會走到這條)。"""
    hh, mm = (int(x) for x in deadline_hhmm.split(":"))
    deadline = datetime.now().replace(hour=hh, minute=mm, second=0, microsecond=0)
    attempt = 0
    while True:
        attempt += 1
        if run_one(d, force=force):
            print(f"[WAIT] 第 {attempt} 次嘗試取得資料")
            log_event({"date": str(d), "attempt": attempt, "ok": True, "mode": "wait"})
            verify_previous(d)
            backfill_institutional(d)
            return True
        if datetime.now() >= deadline:
            print(f"[WAIT-GIVEUP] {d} 至 {deadline_hhmm} 仍無資料(假日或延遲),共嘗試 {attempt} 次")
            log_event({"date": str(d), "attempt": attempt, "ok": False, "mode": "wait",
                       "note": "deadline"})
            return False
        print(f"[WAIT] 第 {attempt} 次無資料,{poll_sec}s 後重試(截止 {deadline_hhmm})")
        time.sleep(poll_sec)


def verify_previous(d):
    """隔日複驗:重抓最近一個已存交易日,與 parquet 比對 OI/結算價是否被官方事後修正。"""
    prev = None
    for f in sorted(TXO_ROOT.glob("quotes/*/TXO_quotes_*.parquet")):
        fd = datetime.strptime(f.stem[-8:], "%Y%m%d").date()
        if fd < d:
            prev = (fd, f)
    if not prev:
        return
    pd_, path = prev
    fresh, _ = build_series(pd_)
    if not fresh:
        print(f"[VERIFY] {pd_} 重抓失敗,跳過複驗")
        return
    old = {(r["exp_code"], r["K"], r["cp"]): (r["oi"], r["settle"])
           for r in pl.read_parquet(path).to_dicts()}
    new = {(s["exp_code"], s["K"], s["cp"]): (s["oi"], s["settle"]) for s in fresh}
    changed = [k for k in old.keys() & new.keys() if old[k] != new[k]]
    added, gone = len(new.keys() - old.keys()), len(old.keys() - new.keys())
    if not changed and not added and not gone:
        print(f"[VERIFY-OK] {pd_} 官方資料與已存版一致({len(old)} 條)")
    else:
        print(f"[VERIFY-WARN] {pd_} 與已存版有出入:值變 {len(changed)} 條、新增 {added}、"
              f"消失 {gone} -> 請跑 --date {pd_} --force 重建")


# ---------------- 入口 ----------------

def run_one(d, force=False, report_only=False):
    qpath = TXO_ROOT / "quotes" / f"{d.year}" / f"TXO_quotes_{d.strftime('%Y%m%d')}.parquet"
    if report_only or (qpath.exists() and not force):
        if not qpath.exists():
            print(f"[SKIP] {d} 無已存 quotes,report-only 無法執行")
            return False
        series = pl.read_parquet(qpath).to_dicts()
        if "spot" not in series[0]:
            print(f"[SKIP] {d} 已存 quotes 是舊格式(無遠期欄位),請用 --force 重建")
            return False
        S = series[0]["spot"]
        meta = {"n_series": len(series), "n_iv_fallback": 0, "S": S, "spot": S,
                "spot_src": "已存 quotes", "fut_front": series[0].get("fut_front", S),
                "basis": series[0].get("fut_front", S) - S}
        stored = False
    else:
        series, meta = build_series(d)
        if not series:
            print(f"[SKIP] {d} 資料未公布或非交易日")
            return False
        S = meta["S"]
        for s in series:                     # 讓 quotes 自帶當日基準,report-only 才能重算
            s["fut_front"] = meta["fut_front"]
        _, stored = store_quotes(d, series, force=force)
    meta['atr'] = atr_txf(d)
    gex = compute_gex(series, meta['fut_front'])
    if report_only:                      # 純重生報告:法人資料讀已存的,不重抓
        ip = TXO_ROOT / "institutional" / f"pc_{d.year}.parquet"
        inst = (pl.read_parquet(ip).filter(pl.col("date") == str(d)).to_dicts()
                if ip.exists() else None) or None
    else:
        inst = fetch_institutional(d)
        if inst:
            store_institutional(d, inst)
    store_summary(d, meta, gex)              # B2:落地每日摘要
    pct = percentiles(d, gex)                # B2:場強百分位
    reconcile(d)                             # B3:前一日地圖 vs 今日實際
    expiries = sorted({(s["exp_code"], s["Td"]) for s in series}, key=lambda x: x[1])
    expiries = [{"code": c, "days": t} for c, t in expiries]
    rpt = render_html(d, S, meta, gex, inst, expiries, pct)
    print(f"[OK] {d} spot={S:,.0f}(基差{meta['basis']:+.0f}) flip={gex['flip_us']} "
          f"totGEX_us={gex['tot_us']:+.1f}(P{pct.get('us','-')}) "
          f"totGEX_tw={gex['tot_tw']:+.1f} inst={'Y' if inst else 'N'} "
          f"quotes={'wrote' if stored else 'cached'} report={rpt.name}")
    return True


class _Tee:
    """排程模式下把 stdout 同時寫進日誌檔(非互動環境看不到主控台輸出)。"""

    def __init__(self, stream, path):
        self.stream, self.f = stream, open(path, "a", encoding="utf-8")

    def write(self, s):
        self.stream.write(s)
        self.f.write(s)
        self.f.flush()

    def flush(self):
        self.stream.flush()
        self.f.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD(預設今天)")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"))
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--wait", action="store_true", help="輪詢等待資料公布(排程用)")
    ap.add_argument("--wait-until", default="16:30", help="輪詢截止時刻 HH:MM")
    ap.add_argument("--poll-sec", type=int, default=180, help="輪詢間隔秒數")
    a = ap.parse_args()
    if a.wait:  # 排程模式:輸出另存日誌
        lp = TXO_ROOT / "logs" / f"run-{date.today()}.log"
        lp.parent.mkdir(parents=True, exist_ok=True)
        sys.stdout = _Tee(sys.stdout, lp)
        print(f"\n===== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 排程啟動 =====")
    if a.backfill:
        d0 = datetime.strptime(a.backfill[0], "%Y-%m-%d").date()
        d1 = datetime.strptime(a.backfill[1], "%Y-%m-%d").date()
        ok = 0
        d = d0
        while d <= d1:
            if d.weekday() < 5:
                if run_one(d, force=a.force, report_only=a.report_only):
                    ok += 1
                time.sleep(2)  # 對 TAIFEX 客氣
            d += timedelta(days=1)
        print(f"[SUMMARY] backfill {d0}~{d1} 完成 {ok} 天")
    else:
        d = datetime.strptime(a.date, "%Y-%m-%d").date() if a.date else date.today()
        if a.wait:
            run_with_wait(d, a.wait_until, a.poll_sec, force=a.force)
        elif run_one(d, force=a.force, report_only=a.report_only):
            log_event({"date": str(d), "attempt": 1, "ok": True, "mode": "manual"})
            verify_previous(d)
            backfill_institutional(d)


if __name__ == "__main__":
    main()
