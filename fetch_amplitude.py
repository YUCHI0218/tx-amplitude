#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台指期(TX)振幅每日追蹤 — 抓「期交所官方」每日行情 CSV、算振幅指標、輸出 amplitude_data.json

★ 資料來源：臺灣期貨交易所 期貨每日交易行情下載（免費、免註冊、免 token）
  端點：POST https://www.taifex.com.tw/cht/3/futDataDown
  （對應網頁：https://www.taifex.com.tw/cht/3/futDailyMarketView）

產出指標（日盤 與 全時段 各一份）：
  - 日振幅：點數(高−低) 與 百分比((高−低)/昨結算×100)
  - MA5 / MA10 / MA20（點數與 % 各一組）
  - 近 20 個交易日 最高 / 最低振幅（附發生日期）
  - 今日振幅落在近一年（預設 250 個交易日）的百分位

前置：
  pip install pandas requests
  不需要任何金鑰。

用法：
  python fetch_amplitude.py                 # 抓期交所真資料（含本地快取）→ amplitude_data.json
  python fetch_amplitude.py --demo          # 合成資料（不連網，看版型用）
  python fetch_amplitude.py --full          # 忽略快取，完整重抓 N 年
  python fetch_amplitude.py --years 3       # 回抓年數（預設 3 年）
  python fetch_amplitude.py --commodity MTX # 改抓小台（預設 TX 大台）

────────────────────────── 期交所資料的兩個關鍵定義（已內建處理） ──────────────────────────
1) 下載區間上限一個月：本程式自動「逐月」抓取再合併。
2) 盤後日期歸屬：期交所明訂——標示交易日期 D 的『盤後』資料，是 D-1 下午 15:00 到 D 凌晨 05:00
   的夜盤。因此同一個 date 的『日盤 + 盤後』本就屬於同一交易日，本程式即以此合併為「全時段」，
   與期交所定義一致。

⚠ 已用「真實的期交所 CSV」驗證：欄位解析（含每列結尾多逗號、CP950 編碼、價差單、盤後結算為 '-'）、
   日盤/盤後切分、近月篩選、振幅計算都對過（例如 2026/07/01 日盤 800 點、全時段 1330 點，與手算一致）。
   唯一沒能在建置沙箱實跑的是「實際對期交所送 HTTP 下載」那一步（沙箱擋外連），但那步產生的檔案格式
   已用真檔確認相符。第一次執行時仍建議看一眼終端輸出的每月列數與最新振幅是否合理。
────────────────────────────────────────────────────────────────────────────────────
"""
import os
import sys
import json
import math
import time
import argparse
import datetime as dt

import pandas as pd

TAIPEI = dt.timezone(dt.timedelta(hours=8))

# trading_session 欄位值 → 標準化為 day / after。期交所用「一般 / 盤後」。
SESSION_MAP = {
    "一般": "day", "position": "day", "regular": "day", "day": "day", "general": "day",
    "盤後": "after", "after_market": "after", "aftermarket": "after", "after": "after",
    "after_hours": "after",
}

PCT_LOOKBACK = 250   # 百分位回看樣本數（約一年交易日）
CHART_DAYS = 120     # 前端時序圖顯示的最近天數
WIN = 20             # 近 N 日最高/最低振幅視窗
COMMODITY = "TX"     # 期交所商品代碼：TX=臺股期貨(大台)，MTX=小型臺指(小台)
TAIFEX_URL = "https://www.taifex.com.tw/cht/3/futDataDown"


def _log(*a):
    print(*a, file=sys.stderr)


def _num(x, n):
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    return round(v, n)


def _parse_date(s: str) -> dt.date:
    s = str(s).strip().replace("-", "/")
    y, m, d = s.split("/")[:3]
    return dt.date(int(y), int(m), int(d))


# ────────────────────────── 抓期交所官方 CSV ──────────────────────────
def _month_ranges(start: dt.date, end: dt.date):
    """逐「日曆月」切段（期交所下載上限為一個月）。"""
    cur = dt.date(start.year, start.month, 1)
    cur = max(cur, start)
    while cur <= end:
        nxt = dt.date(cur.year + 1, 1, 1) if cur.month == 12 else dt.date(cur.year, cur.month + 1, 1)
        yield cur, min(end, nxt - dt.timedelta(days=1))
        cur = nxt


# 期交所 CSV 中文欄名 → 內部英文欄名。(exact=True 需完全相符；False 為包含即可)
_COLMAP = {
    "date": (["交易日期"], True),
    "futures_id": (["契約"], True),
    "contract_date": (["到期月份"], False),
    "open": (["開盤價"], True),
    "max": (["最高價"], True),
    "min": (["最低價"], True),
    "close": (["收盤價"], True),
    "volume": (["成交量"], True),           # exact，避免誤中「價差對單式委託成交量」
    "settlement_price": (["結算價"], True),
    "open_interest": (["未沖銷契約數"], True),
    "trading_session": (["交易時段"], False),
}


def _rename_taifex(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

    def find(cands, exact):
        for c in df.columns:
            for k in cands:
                if (c == k) if exact else (k in c):
                    return c
        return None

    ren = {}
    for tgt, (cands, exact) in _COLMAP.items():
        col = find(cands, exact)
        if col:
            ren[col] = tgt
    df = df.rename(columns=ren)

    for c in ["open", "max", "min", "close", "volume", "settlement_price", "open_interest"]:
        if c in df.columns:
            s = (df[c].astype(str).str.replace(",", "", regex=False).str.strip()
                 .replace({"-": None, "": None, "nan": None}))
            df[c] = pd.to_numeric(s, errors="coerce")
    for c in ["date", "trading_session", "futures_id"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def fetch_taifex_range(start: dt.date, end: dt.date) -> pd.DataFrame:
    import io
    import requests
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.taifex.com.tw/cht/3/futDailyMarketView",
    })
    frames = []
    for s, e in _month_ranges(start, end):
        payload = {
            "down_type": "1",
            "commodity_id": COMMODITY,
            "commodity_id2": "",
            "queryStartDate": s.strftime("%Y/%m/%d"),
            "queryEndDate": e.strftime("%Y/%m/%d"),
        }
        try:
            r = sess.post(TAIFEX_URL, data=payload, timeout=60)
            r.raise_for_status()
            raw = r.content.decode("cp950", "replace")
        except Exception as ex:  # noqa: BLE001
            _log(f"  {s}~{e}: 抓取失敗（{ex}），略過")
            continue
        if "交易日期" not in raw:
            _log(f"  {s}~{e}: 回應中找不到表頭，略過")
            continue
        df = pd.read_csv(io.StringIO(raw), index_col=False)
        if len(df):
            frames.append(df)
            _log(f"  {s}~{e}: {len(df)} 列")
        time.sleep(0.3)  # 對期交所客氣一點
    if not frames:
        raise SystemExit("期交所沒有抓到任何資料。可能端點/參數改版或被限流，請看 README「核對」段。")
    return _rename_taifex(pd.concat(frames, ignore_index=True))


def front_rows(df: pd.DataFrame) -> pd.DataFrame:
    """把原始多合約資料縮成『每個 (交易日, 盤別) 一列近月』，供快取與計算。"""
    n = _normalize(df.copy())
    return _front_month(n)


def load_data(years: int, cache_path: str, force_full: bool = False) -> pd.DataFrame:
    end = dt.date.today()
    if (not force_full) and os.path.exists(cache_path):
        old = pd.read_csv(cache_path)
        old["date"] = old["date"].astype(str)
        try:
            last = _parse_date(old["date"].max())
        except Exception:  # noqa: BLE001
            last = end - dt.timedelta(days=int(years * 372))
        start = last - dt.timedelta(days=7)   # 重抓最近幾天以吸收更正
        _log(f"增量更新：自 {start} 起補抓（快取最後日 {old['date'].max()}）")
        new = front_rows(fetch_taifex_range(start, end))
        allrows = pd.concat([old, new], ignore_index=True)
    else:
        start = end - dt.timedelta(days=int(years * 372))
        n_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
        _log(f"完整回補：{start} ~ {end}（逐月抓約 {n_months} 次，請稍候）")
        allrows = front_rows(fetch_taifex_range(start, end))

    allrows = (allrows.drop_duplicates(subset=["date", "trading_session"], keep="last")
               .sort_values("date").reset_index(drop=True))
    allrows.to_csv(cache_path, index=False)

    n_after = int((allrows.get("trading_session", pd.Series(dtype=str)) == "盤後").sum())
    if n_after == 0:
        _log("⚠ 找不到任何『盤後』資料 → 全時段將等於日盤。請確認下載是否含夜盤（見 README）。")
    return allrows


# ────────────────────────── 計算（純函式，已用合成資料逐項驗證） ──────────────────────────
def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: c.lower() for c in df.columns}).copy()
    need = ["date", "max", "min", "close", "volume", "settlement_price", "trading_session"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"缺少欄位 {missing}；實際欄位為 {list(df.columns)}")
    for c in ["open", "max", "min", "close", "volume", "settlement_price"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["sess"] = df["trading_session"].astype(str).str.strip().str.lower().map(
        {k.lower(): v for k, v in SESSION_MAP.items()})
    unknown = sorted(set(df.loc[df["sess"].isna(), "trading_session"].astype(str)))
    if unknown:
        _log(f"⚠ trading_session 有未對應的值：{unknown} — 請更新 SESSION_MAP。這些列暫當日盤。")
        df["sess"] = df["sess"].fillna("day")
    return df


def _front_month(df: pd.DataFrame) -> pd.DataFrame:
    """每個 (date, sess) 取成交量最大的合約 = 近月。低量價差單與遠月自然被跳過，
    換倉當天次月量能超越也會自動接手，不必手算結算日。
    另外明確排除價差單（到期月份含 '/'，例如 202607/202608，其價格是月間價差非指數點位）。"""
    if "contract_date" in df.columns:
        df = df[~df["contract_date"].astype(str).str.contains("/", na=False)]
    df = df.dropna(subset=["volume"])
    df = df[df["volume"] > 0]
    idx = df.groupby(["date", "sess"])["volume"].idxmax()
    return df.loc[idx]


def build_sessions(df: pd.DataFrame):
    df = _normalize(df)
    fm = _front_month(df)
    day = fm[fm["sess"] == "day"].set_index("date")
    aft = fm[fm["sess"] == "after"].set_index("date")

    rows = []
    for d in sorted(day.index):
        dr = day.loc[d]
        d_hi, d_lo = dr["max"], dr["min"]
        d_op, d_cl = dr["open"], dr["close"]
        if d in aft.index:
            ar = aft.loc[d]
            n_op, n_hi, n_lo, n_cl = ar["open"], ar["max"], ar["min"], ar["close"]
            f_hi, f_lo, f_op = max(d_hi, ar["max"]), min(d_lo, ar["min"]), ar["open"]  # 夜盤先開
        else:
            n_op = n_hi = n_lo = n_cl = float("nan")  # 當天無夜盤資料
            f_hi, f_lo, f_op = d_hi, d_lo, d_op
        rows.append({"date": d,
                     "d_op": d_op, "d_hi": d_hi, "d_lo": d_lo, "d_cl": d_cl,
                     "n_op": n_op, "n_hi": n_hi, "n_lo": n_lo, "n_cl": n_cl,
                     "f_op": f_op, "f_hi": f_hi, "f_lo": f_lo, "f_cl": d_cl,
                     "settle": dr["settlement_price"], "close": dr["close"]})
    b = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    b["ref"] = b["settle"].shift(1)
    b["ref"] = b["ref"].fillna(b["close"].shift(1))

    def make(op, hi, lo, cl):
        s = pd.DataFrame({"date": b["date"]})
        s["open"] = op
        s["high"] = hi
        s["low"] = lo
        s["close"] = cl
        s["amp_pt"] = hi - lo
        s["amp_pct"] = (s["amp_pt"] / b["ref"] * 100).where(b["ref"].fillna(0) != 0)
        return s

    return (make(b["d_op"], b["d_hi"], b["d_lo"], b["d_cl"]),
            make(b["n_op"], b["n_hi"], b["n_lo"], b["n_cl"]),
            make(b["f_op"], b["f_hi"], b["f_lo"], b["f_cl"]))


def summarize(s: pd.DataFrame, label: str) -> dict:
    s = s.sort_values("date").reset_index(drop=True)
    for w in (5, 10, 20):
        s[f"ma{w}_pt"] = s["amp_pt"].rolling(w, min_periods=w).mean()
        s[f"ma{w}_pct"] = s["amp_pct"].rolling(w, min_periods=w).mean()

    latest = s.iloc[-1]
    last_win = s.tail(WIN)

    def extreme(frame, col, how):
        idx = frame[col].idxmax() if how == "max" else frame[col].idxmin()
        r = frame.loc[idx]
        return {"date": r["date"], "amp_pt": _num(r["amp_pt"], 0), "amp_pct": _num(r["amp_pct"], 2)}

    lb = s.tail(PCT_LOOKBACK)

    def pctile(col, val):
        arr = lb[col].dropna()
        if len(arr) == 0 or (isinstance(val, float) and math.isnan(val)):
            return None
        return _num((arr <= val).sum() / len(arr) * 100, 1)

    series = s.tail(CHART_DAYS)
    return {
        "label": label,
        "latest": {
            "date": latest["date"],
            "amp_pt": _num(latest["amp_pt"], 0),
            "amp_pct": _num(latest["amp_pct"], 2),
            "high": _num(latest["high"], 0),
            "low": _num(latest["low"], 0),
        },
        "ma": {
            "pt": {f"ma{w}": _num(latest[f"ma{w}_pt"], 1) for w in (5, 10, 20)},
            "pct": {f"ma{w}": _num(latest[f"ma{w}_pct"], 2) for w in (5, 10, 20)},
        },
        "window20": {
            "n": int(len(last_win)),
            "max_pt": extreme(last_win, "amp_pt", "max"),
            "min_pt": extreme(last_win, "amp_pt", "min"),
            "max_pct": extreme(last_win, "amp_pct", "max"),
            "min_pct": extreme(last_win, "amp_pct", "min"),
        },
        "percentile": {
            "lookback": int(min(PCT_LOOKBACK, len(s))),
            "pt": pctile("amp_pt", latest["amp_pt"]),
            "pct": pctile("amp_pct", latest["amp_pct"]),
        },
        "series": [
            {
                "date": r["date"],
                "open": _num(r["open"], 0),
                "high": _num(r["high"], 0),
                "low": _num(r["low"], 0),
                "close": _num(r["close"], 0),
                "amp_pt": _num(r["amp_pt"], 0),
                "amp_pct": _num(r["amp_pct"], 2),
                **{f"ma{w}_pt": _num(r[f"ma{w}_pt"], 1) for w in (5, 10, 20)},
                **{f"ma{w}_pct": _num(r[f"ma{w}_pct"], 2) for w in (5, 10, 20)},
            }
            for _, r in series.iterrows()
        ],
    }


def compute_metrics(df: pd.DataFrame) -> dict:
    day_s, night_s, full_s = build_sessions(df)
    name = "臺股期貨 TX（大台）" if COMMODITY == "TX" else ("小型臺指 MTX（小台）" if COMMODITY == "MTX" else COMMODITY)
    return {
        "generated_at": dt.datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "instrument": f"{name} · 近月連續（每日取成交量最大合約）· 資料來源：臺灣期貨交易所",
        "defs": "日振幅 = 當日最高 − 最低；% = (高−低) ÷ 昨結算 × 100。日盤=08:45–13:45；夜盤=前一日15:00至當日05:00；全時段=兩者合併。",
        "sessions": {
            "day": summarize(day_s, "日盤 08:45–13:45"),
            "night": summarize(night_s, "夜盤 15:00–05:00"),
            "full": summarize(full_s, "全時段（含夜盤）"),
        },
    }


# ────────────────────────── 合成資料（--demo，期交所欄位格式） ──────────────────────────
def _synthetic_raw(years: int) -> pd.DataFrame:
    import random
    random.seed(7)
    n, made, level = int(years * 245), 0, 18000.0
    d = dt.date.today() - dt.timedelta(days=int(n * 1.45))
    rows = []
    while made < n:
        if d.weekday() < 5:  # 只取週一到週五（demo 不管台股假日）
            base = 130 + 95 * math.sin(made / 23.0) + random.gauss(0, 38)
            rng = max(35, base + (110 if random.random() < 0.06 else 0))
            close = level + random.gauss(0, 115)
            hi = max(level, close) + random.uniform(0.35, 0.7) * rng
            lo = hi - rng
            settle = round(close)
            ym = f"{d.year}{d.month:02d}"
            ds = d.strftime("%Y/%m/%d")

            def row(sess, o, h, l, c, v, oi):
                return {"交易日期": ds, "契約": "TX", "到期月份(週別)": ym,
                        "開盤價": f"{o:,.0f}", "最高價": f"{h:,.0f}", "最低價": f"{l:,.0f}",
                        "收盤價": f"{c:,.0f}", "漲跌價": "0", "漲跌%": "0",
                        "成交量": f"{v:,}", "結算價": f"{settle:,.0f}", "未沖銷契約數": f"{oi:,}",
                        "交易時段": sess, "價差對單式委託成交量": "0"}

            rows.append(row("一般", level, hi, lo, close, random.randint(90000, 160000), random.randint(60000, 90000)))
            ex = rng * random.uniform(0.15, 0.8)
            rows.append(row("盤後", close,
                            hi + (ex if random.random() < 0.5 else 0),
                            lo - (ex if random.random() < 0.5 else 0),
                            close + random.gauss(0, 60), random.randint(20000, 45000), 0))
            # 低量價差單（價格以 "-" 表示），測試近月篩選與數值清理
            rows.append({"交易日期": ds, "契約": "TX", "到期月份(週別)": f"{ym}/{d.year}{(d.month % 12) + 1:02d}",
                         "開盤價": "-", "最高價": "-", "最低價": "-", "收盤價": "-", "漲跌價": "-",
                         "漲跌%": "-", "成交量": f"{random.randint(50, 400)}", "結算價": "-",
                         "未沖銷契約數": "0", "交易時段": "一般", "價差對單式委託成交量": "0"})
            level = close
            made += 1
        d += dt.timedelta(days=1)
    return pd.DataFrame(rows)


def _self_contained_dashboard(out_json_path: str, script_dir: str):
    """把最新資料直接嵌進 dashboard 範本，另存 dashboard_latest.html。
    這個檔『自帶資料』，雙擊就能看最新數字（不需開伺服器），很適合放 Google Drive。"""
    import re
    outdir = os.path.dirname(os.path.abspath(out_json_path)) or "."
    template = None
    for cand in (os.path.join(outdir, "dashboard.html"), os.path.join(script_dir, "dashboard.html")):
        if os.path.exists(cand):
            template = cand
            break
    if not template:
        _log("（找不到 dashboard.html 範本，略過 dashboard_latest.html）")
        return
    html = open(template, encoding="utf-8").read()
    data = open(out_json_path, encoding="utf-8").read()
    new = re.sub(r'(<script id="fallback" type="application/json">).*?(</script>)',
                 lambda m: m.group(1) + data + m.group(2), html, count=1, flags=re.S)
    dest = os.path.join(outdir, "dashboard_latest.html")
    with open(dest, "w", encoding="utf-8") as f:
        f.write(new)
    _log(f"✓ 自包含儀表板：{dest}（雙擊即可看最新資料）")


def main():
    global COMMODITY
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="用合成資料（不連期交所）")
    ap.add_argument("--years", type=int, default=3)
    ap.add_argument("--out", default="amplitude_data.json")
    ap.add_argument("--cache", default="taifex_tx_cache.csv")
    ap.add_argument("--full", action="store_true", help="忽略快取，完整重抓")
    ap.add_argument("--commodity", default="TX", help="TX=大台，MTX=小台")
    ap.add_argument("--no-dashboard", action="store_true", help="不要產生 dashboard_latest.html")
    a = ap.parse_args()
    COMMODITY = a.commodity

    if a.demo:
        df = _rename_taifex(_synthetic_raw(a.years))
    else:
        df = load_data(a.years, a.cache, force_full=a.full)

    data = compute_metrics(df)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    if not a.no_dashboard:
        _self_contained_dashboard(a.out, os.path.dirname(os.path.abspath(__file__)))

    dd, ff = data["sessions"]["day"]["latest"], data["sessions"]["full"]["latest"]
    _log(f"✓ 寫出 {a.out}")
    _log(f"  最新 {dd['date']}｜日盤 {dd['amp_pt']}點 / {dd['amp_pct']}%｜全時段 {ff['amp_pt']}點 / {ff['amp_pct']}%")


if __name__ == "__main__":
    main()
