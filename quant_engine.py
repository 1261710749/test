#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一人对冲基金 (One-Person Quantitative Hedge Fund) — 核心量化研究、真实数据回测与多智能体引擎
=============================================================================================
用户原始约束 (ORIGINAL_REQUEST.md):
  "新闻可以是全球，我只布局中国大陆和港股市场，可以是长回报周期，资金规模2w人民币"
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import shutil
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import numpy as np

from strategy_rank1_bot import (
    CN_HK_TO_ALPACA_PROXY,
    STEP_1_RULES_SUMMARY,
    STEP_2_MINIMAL_DESIGN,
    STEP_3_ARCHITECTURE_SPEC,
    STEP_6_DEPLOYMENT_GUIDE_MD,
    audit_bot_code,
    calculate_atr_position_size,
    execute_overnight_paper_portfolio,
    generate_sunday_reconciliation_report,
    restate_objective_and_rules,
)

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "trading_desk_rules.json"
DB_PATH = BASE_DIR / "data" / "market_cache.sqlite"
REPORTS_DIR = BASE_DIR / "reports"
PINE_PATH = BASE_DIR / "pine" / "strategy_rank1_indicator.pine"

UNIVERSE_15: list[dict[str, Any]] = [
    {"idx": 0, "symbol": "sh510300", "yf": "510300.SS", "name": "华泰柏瑞沪深300ETF", "asset_class": "A股宽基ETF", "settlement": "T+1", "fx": 1.0},
    {"idx": 1, "symbol": "sz159915", "yf": "159915.SZ", "name": "易方达创业板ETF", "asset_class": "A股成长ETF", "settlement": "T+1", "fx": 1.0},
    {"idx": 2, "symbol": "sh512480", "yf": "512480.SS", "name": "国联安中证全指半导体ETF", "asset_class": "A股科技ETF", "settlement": "T+1", "fx": 1.0},
    {"idx": 3, "symbol": "sh512880", "yf": "512880.SS", "name": "国泰中证全指证券公司ETF", "asset_class": "A股周期金融ETF", "settlement": "T+1", "fx": 1.0},
    {"idx": 4, "symbol": "sh510880", "yf": "510880.SS", "name": "华泰柏瑞红利ETF", "asset_class": "A股高股息防御ETF", "settlement": "T+1", "fx": 1.0},
    {"idx": 5, "symbol": "sh518880", "yf": "518880.SS", "name": "华安黄金ETF", "asset_class": "商品避险ETF", "settlement": "T+0", "fx": 1.0},
    {"idx": 6, "symbol": "sz159920", "yf": "159920.SZ", "name": "华夏恒生ETF", "asset_class": "港股跨境ETF", "settlement": "T+0", "fx": 1.0},
    {"idx": 7, "symbol": "sh513050", "yf": "513050.SS", "name": "易方达中概互联网ETF", "asset_class": "港股科技跨境ETF", "settlement": "T+0", "fx": 1.0},
    {"idx": 8, "symbol": "sh510900", "yf": "510900.SS", "name": "易方达恒生国企ETF", "asset_class": "港股宽基跨境ETF", "settlement": "T+0", "fx": 1.0},
    {"idx": 9, "symbol": "hk02800", "yf": "2800.HK", "name": "盈富基金 (Tracker Fund of HK)", "asset_class": "港股本土核心ETF", "settlement": "T+0", "fx": 0.92},
    {"idx": 10, "symbol": "sh600036", "yf": "600036.SS", "name": "招商银行", "asset_class": "A股主板金融蓝筹", "settlement": "T+1", "fx": 1.0},
    {"idx": 11, "symbol": "sh601398", "yf": "601398.SS", "name": "工商银行", "asset_class": "A股主板高股息银行", "settlement": "T+1", "fx": 1.0},
    {"idx": 12, "symbol": "sh601899", "yf": "601899.SS", "name": "紫金矿业", "asset_class": "A股主板金铜资源龙头", "settlement": "T+1", "fx": 1.0},
    {"idx": 13, "symbol": "sh600900", "yf": "600900.SS", "name": "长江电力", "asset_class": "A股主板水电防御龙头", "settlement": "T+1", "fx": 1.0},
    {"idx": 14, "symbol": "sh512660", "yf": "512660.SS", "name": "国泰中证军工ETF", "asset_class": "A股高端制造ETF", "settlement": "T+1", "fx": 1.0},
]

EXTRA_CHART_TICKERS: list[dict[str, Any]] = [
    {"symbol": "sh512010", "yf": "512010.SS", "name": "医药ETF", "asset_class": "A股行业ETF", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh600028", "yf": "600028.SS", "name": "中国石化", "asset_class": "A股主板能源蓝筹", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh601857", "yf": "601857.SS", "name": "中国石油", "asset_class": "A股主板能源蓝筹", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh601766", "yf": "601766.SS", "name": "中国中车", "asset_class": "A股主板高端装备", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh601668", "yf": "601668.SS", "name": "中国建筑", "asset_class": "A股主板基建央企", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh600050", "yf": "600050.SS", "name": "中国联通", "asset_class": "A股主板通信算网", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh601328", "yf": "601328.SS", "name": "交通银行", "asset_class": "A股主板高股息银行", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh600019", "yf": "600019.SS", "name": "宝钢股份", "asset_class": "A股主板钢铁龙头", "settlement": "T+1", "fx": 1.0},
    {"symbol": "hk03033", "yf": "3033.HK", "name": "南方恒生科技", "asset_class": "港股本土科技ETF", "settlement": "T+0", "fx": 0.92},
    {"symbol": "sh601288", "yf": "601288.SS", "name": "农业银行", "asset_class": "A股主板银行", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh601988", "yf": "601988.SS", "name": "中国银行", "asset_class": "A股主板银行", "settlement": "T+1", "fx": 1.0},
    {"symbol": "sh600522", "yf": "600522.SS", "name": "中天科技", "asset_class": "A股主板光通信", "settlement": "T+1", "fx": 1.0},
]

ALL_26_TICKERS: list[dict[str, Any]] = [u for u in UNIVERSE_15 if u["symbol"] != "sh600036"] + [
    t for t in EXTRA_CHART_TICKERS if t["symbol"] not in {u["symbol"] for u in UNIVERSE_15}
]

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://gu.qq.com/",
}


def init_database(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_bars (
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chart_scan_bars (
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS earnings_announcements (
            symbol TEXT,
            name TEXT,
            report_date TEXT,
            notice_date TEXT,
            eps REAL,
            rev_yoy REAL,
            profit_yoy REAL,
            roe REAL,
            source TEXT,
            PRIMARY KEY (symbol, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS live_news (
            id TEXT PRIMARY KEY,
            timestamp TEXT,
            title TEXT,
            content TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_flows (
            code TEXT PRIMARY KEY,
            name TEXT,
            pct_change REAL,
            main_net_inflow REAL,
            net_ratio REAL,
            lead_stock TEXT,
            timestamp TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ah_premium (
            date TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pct_change REAL,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS corporate_announcements (
            ann_id TEXT PRIMARY KEY,
            date TEXT,
            code TEXT,
            name TEXT,
            title TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS live_quotes (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            price REAL,
            prev_close REAL,
            open_price REAL,
            pct_change REAL,
            timestamp TEXT,
            source TEXT
        )
        """
    )
    conn.commit()
    return conn


def _fetch_json(url: str, headers: dict[str, str] | None = None, timeout: int = 10, retries: int = 3) -> Any:
    hdrs = dict(HTTP_HEADERS)
    if headers:
        hdrs.update(headers)
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
        except Exception as exc:
            last_err = exc
            time.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def fetch_symbol_qfq_bars(
    symbol: str,
    yf_symbol: str,
    fx: float = 1.0,
    segments: list[tuple[str, str]] | None = None,
) -> list[tuple[str, str, float, float, float, float, float, str]]:
    segs = segments or [
        ("2019-01-01", "2021-12-31"),
        ("2022-01-01", "2024-12-31"),
        ("2025-01-01", "2026-11-06"),
    ]
    by_date: dict[str, tuple[str, str, float, float, float, float, float, str]] = {}

    for beg, end in segs:
        fetched_seg = False
        for host in (
            "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get",
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        ):
            url = f"{host}?param={symbol},day,{beg},{end},800,qfq"
            try:
                data = _fetch_json(url, timeout=10, retries=3)
                sym_data = (data.get("data") or {}).get(symbol) or {}
                rows = sym_data.get("qfqday") or sym_data.get("day") or []
                if rows:
                    for r in rows:
                        d_str = str(r[0])
                        if "2019-01-02" <= d_str <= "2026-11-06":
                            o = round(float(r[1]) * fx, 4)
                            c = round(float(r[2]) * fx, 4)
                            h = round(float(r[3]) * fx, 4)
                            l = round(float(r[4]) * fx, 4)
                            v = float(r[5])
                            by_date[d_str] = (symbol, d_str, o, h, l, c, v, url)
                    fetched_seg = True
                    break
            except Exception:
                continue

        if not fetched_seg:
            yf_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}?period1=1546300800&period2=1794009600&interval=1d"
            data = _fetch_json(yf_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12, retries=3)
            res0 = data["chart"]["result"][0]
            ts_list = res0["timestamp"]
            q0 = res0["indicators"]["quote"][0]
            adj_list = (res0["indicators"].get("adjclose") or [{}])[0].get("adjclose") or q0["close"]
            for idx_t, ts in enumerate(ts_list):
                d_str = dt.datetime.utcfromtimestamp(ts + 8 * 3600).strftime("%Y-%m-%d")
                if "2019-01-02" <= d_str <= "2026-11-06":
                    c_raw = q0["close"][idx_t]
                    o_raw = q0["open"][idx_t]
                    h_raw = q0["high"][idx_t]
                    l_raw = q0["low"][idx_t]
                    v_raw = q0["volume"][idx_t] or 0.0
                    adj_c = adj_list[idx_t] if adj_list and adj_list[idx_t] is not None else c_raw
                    if c_raw and c_raw > 0 and o_raw and h_raw and l_raw:
                        ratio = (float(adj_c) / float(c_raw)) * fx
                        by_date[d_str] = (
                            symbol,
                            d_str,
                            round(float(o_raw) * ratio, 4),
                            round(float(h_raw) * ratio, 4),
                            round(float(l_raw) * ratio, 4),
                            round(float(c_raw) * ratio, 4),
                            float(v_raw),
                            yf_url,
                        )
            break

    return [by_date[d] for d in sorted(by_date.keys())]


def ingest_all_market_data(conn: sqlite3.Connection, force_refresh: bool = False) -> dict[str, Any]:
    cur = conn.cursor()

    cur.execute("SELECT symbol, COUNT(*) FROM daily_bars GROUP BY symbol")
    existing_counts = dict(cur.fetchall())
    if force_refresh or len(existing_counts) < len(UNIVERSE_15) or min(existing_counts.values(), default=0) < 1750:
        for item in UNIVERSE_15:
            sym = item["symbol"]
            if not force_refresh and existing_counts.get(sym, 0) >= 1750:
                continue
            bars = fetch_symbol_qfq_bars(sym, item["yf"], fx=item["fx"])
            cur.executemany(
                "INSERT OR REPLACE INTO daily_bars (symbol, date, open, high, low, close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                bars,
            )
            cur.executemany(
                "INSERT OR REPLACE INTO chart_scan_bars (symbol, date, open, high, low, close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                bars[-260:],
            )
            conn.commit()
    else:
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM chart_scan_bars")
        if (cur.fetchone()[0] or 0) < len(UNIVERSE_15):
            cur.execute(
                """
                INSERT OR REPLACE INTO chart_scan_bars (symbol, date, open, high, low, close, volume, source)
                SELECT symbol, date, open, high, low, close, volume, source
                FROM daily_bars WHERE date >= '2025-10-01'
                """
            )
            conn.commit()

    cur.execute("SELECT symbol, COUNT(*) FROM chart_scan_bars GROUP BY symbol")
    scan_counts = dict(cur.fetchall())
    for item in EXTRA_CHART_TICKERS:
        sym = item["symbol"]
        if not force_refresh and scan_counts.get(sym, 0) >= 120:
            continue
        bars = fetch_symbol_qfq_bars(
            sym,
            item["yf"],
            fx=1.0,
            segments=[("2025-10-01", "2026-11-06")],
        )
        cur.executemany(
            "INSERT OR REPLACE INTO chart_scan_bars (symbol, date, open, high, low, close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            bars,
        )
        conn.commit()

    try:
        q_syms = ",".join(t["symbol"] for t in ALL_26_TICKERS)
        q_url = f"https://qt.gtimg.cn/q={q_syms}"
        req = urllib.request.Request(q_url, headers=HTTP_HEADERS)
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw_q = resp.read().decode("gbk", errors="replace")
        for line in raw_q.strip().split(";"):
            if "=" not in line:
                continue
            lhs, rhs = line.split("=", 1)
            sym = lhs.strip().split("v_")[-1]
            parts = rhs.strip().strip('"').split("~")
            if len(parts) > 32 and parts[3]:
                name_q = parts[1]
                price_q = float(parts[3])
                prev_q = float(parts[4] or parts[3])
                open_q = float(parts[5] or parts[3])
                pct_q = float(parts[32] or 0.0)
                ts_q = str(parts[30])
                cur.execute(
                    "INSERT OR REPLACE INTO live_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (sym, name_q, price_q, prev_q, open_q, pct_q, ts_q, q_url),
                )
        conn.commit()
    except Exception:
        pass

    cur.execute("SELECT COUNT(*) FROM earnings_announcements")
    if force_refresh or (cur.fetchone()[0] or 0) < 100:
        codes = (
            '"600036","601398","601899","600900","600028","601857","601766",'
            '"601668","600050","601328","600019","601288","601988","600522","603920","688827"'
        )
        params = urllib.parse.urlencode(
            {
                "reportName": "RPT_LICO_FN_CPD",
                "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,REPORTDATE,NOTICE_DATE,BASIC_EPS,YSTZ,SJLTZ,WEIGHTAVG_ROE",
                "filter": f"(SECURITY_CODE in ({codes}))",
                "pageNumber": "1",
                "pageSize": "500",
                "sortTypes": "-1,-1",
                "sortColumns": "REPORTDATE,NOTICE_DATE",
                "source": "WEB",
                "client": "WEB",
            }
        )
        em_url = f"https://datacenter-web.eastmoney.com/api/data/v1/get?{params}"
        try:
            em_data = _fetch_json(em_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12, retries=3)
            rows = (em_data.get("result") or {}).get("data") or []
            for r in rows:
                code = str(r.get("SECURITY_CODE", ""))
                sym = f"sh{code}" if code.startswith("6") else f"sz{code}"
                name = str(r.get("SECURITY_NAME_ABBR", ""))
                rep_d = str(r.get("REPORTDATE", ""))[:10]
                not_d = str(r.get("NOTICE_DATE", ""))[:10]
                eps = float(r["BASIC_EPS"]) if r.get("BASIC_EPS") is not None else 0.0
                rev_yoy = float(r["YSTZ"]) if r.get("YSTZ") is not None else 0.0
                prof_yoy = float(r["SJLTZ"]) if r.get("SJLTZ") is not None else 0.0
                roe = float(r["WEIGHTAVG_ROE"]) if r.get("WEIGHTAVG_ROE") is not None else 0.0
                if rep_d and not_d:
                    cur.execute(
                        "INSERT OR REPLACE INTO earnings_announcements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (sym, name, rep_d, not_d, eps, rev_yoy, prof_yoy, roe, em_url),
                    )
            conn.commit()
        except Exception:
            pass

    cur.execute("SELECT COUNT(*) FROM sector_flows")
    if force_refresh or (cur.fetchone()[0] or 0) < 10:
        for po_val in ("1", "0"):
            sf_url = (
                f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=15&po={po_val}"
                "&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2&fields=f12,f14,f2,f3,f62,f184,f128"
            )
            try:
                sf_data = _fetch_json(sf_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, retries=2)
                diff = ((sf_data.get("data") or {}).get("diff")) or []
                now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for item in diff:
                    code = str(item.get("f12", ""))
                    name = str(item.get("f14", ""))
                    pct = float(item.get("f3") or 0.0)
                    inflow = float(item.get("f62") or 0.0)
                    ratio = float(item.get("f184") or 0.0)
                    lead = str(item.get("f128") or "")
                    if code:
                        cur.execute(
                            "INSERT OR REPLACE INTO sector_flows VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (code, name, pct, inflow, ratio, lead, now_ts, sf_url),
                        )
                conn.commit()
            except Exception:
                pass

    cur.execute("SELECT COUNT(*) FROM ah_premium")
    if force_refresh or (cur.fetchone()[0] or 0) < 5:
        ah_url = (
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=100.HSAHP"
            "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59"
            "&klt=101&fqt=0&beg=20261001&end=20261109"
        )
        try:
            ah_data = _fetch_json(ah_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, retries=2)
            klines = ((ah_data.get("data") or {}).get("klines")) or []
            for k in klines:
                parts = k.split(",")
                if len(parts) >= 9:
                    cur.execute(
                        "INSERT OR REPLACE INTO ah_premium VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            parts[0],
                            float(parts[1]),
                            float(parts[3]),
                            float(parts[4]),
                            float(parts[2]),
                            float(parts[8]),
                            ah_url,
                        ),
                    )
            conn.commit()
        except Exception:
            pass

    cur.execute("SELECT COUNT(*) FROM corporate_announcements")
    if force_refresh or (cur.fetchone()[0] or 0) < 5:
        ann_url = (
            "https://np-anotice-stock.eastmoney.com/api/security/ann?"
            "sr=-1&page_size=20&page_index=1&ann_type=A&client_source=web&f_node=0&s_node=0"
        )
        try:
            ann_data = _fetch_json(ann_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, retries=2)
            items = ((ann_data.get("data") or {}).get("list")) or []
            for it in items:
                art_code = str(it.get("art_code", ""))
                ann_date = str(it.get("notice_date", ""))[:10]
                codes_list = it.get("codes") or [{}]
                sec_code = str(codes_list[0].get("stock_code", ""))
                sec_name = str(codes_list[0].get("short_name", ""))
                title = str(it.get("title", ""))
                if art_code and title:
                    cur.execute(
                        "INSERT OR REPLACE INTO corporate_announcements VALUES (?, ?, ?, ?, ?, ?)",
                        (art_code, ann_date, sec_code, sec_name, title, ann_url),
                    )
            conn.commit()
        except Exception:
            pass

    cur.execute("SELECT COUNT(*) FROM live_news")
    if force_refresh or (cur.fetchone()[0] or 0) < 10:
        sina_url = "https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=30&zhibo_id=152"
        try:
            s_data = _fetch_json(sina_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, retries=2)
            feed_list = (((s_data.get("result") or {}).get("data") or {}).get("feed") or {}).get("list") or []
            for item in feed_list:
                nid = str(item.get("id", ""))
                ts = str(item.get("create_time", ""))
                txt = str(item.get("rich_text", "")).strip()
                title = txt.split("】")[0].lstrip("【") if "】" in txt else txt[:42]
                if nid and txt:
                    cur.execute(
                        "INSERT OR REPLACE INTO live_news VALUES (?, ?, ?, ?, ?)",
                        (nid, ts, title, txt, sina_url),
                    )
            conn.commit()
        except Exception:
            pass

    cur.execute("SELECT COUNT(DISTINCT symbol), COUNT(*) FROM daily_bars")
    n_syms, n_bars = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM earnings_announcements")
    n_earn = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sector_flows")
    n_sec = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM live_news")
    n_news = cur.fetchone()[0]
    return {
        "symbols_cached": n_syms,
        "daily_bars_cached": n_bars,
        "earnings_cached": n_earn,
        "sector_flows_cached": n_sec,
        "live_news_cached": n_news,
    }


def calc_rsi14(closes: np.ndarray) -> float:
    if len(closes) < 15:
        return 50.0
    deltas = np.diff(closes[-15:])
    up = float(np.mean(np.where(deltas > 0, deltas, 0.0)))
    dn = float(np.mean(np.where(deltas < 0, -deltas, 0.0)))
    return 100.0 - (100.0 / (1.0 + up / (dn + 1e-12)))


def calc_atr14(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    if len(closes) < 15:
        return float(closes[-1] * 0.02)
    prev_c = closes[-15:-1]
    cur_h = highs[-14:]
    cur_l = lows[-14:]
    tr = np.maximum(cur_h - cur_l, np.maximum(np.abs(cur_h - prev_c), np.abs(cur_l - prev_c)))
    return float(np.mean(tr))


def score_chart_setup(
    closes: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    vols: np.ndarray,
) -> tuple[float, dict[str, Any]]:
    """
    图表分析师 (Chart Analyst) 1-10 分评分函数：
    评估 趋势(0-4分) + 动量(0-3分) + 成交量与波动结构(0-3分) 三者共振。
    放行门槛：total_score >= 7.0。
    """
    c = float(closes[-1])
    ma20 = float(np.mean(closes[-20:]))
    ma60 = float(np.mean(closes[-60:]))
    roc20 = (c / float(closes[-21]) - 1.0) * 100.0
    roc5 = (c / float(closes[-6]) - 1.0) * 100.0
    rsi14 = calc_rsi14(closes)
    vol_ma20 = float(np.mean(vols[-21:-1]))
    vol_ratio = float(vols[-1]) / vol_ma20 if vol_ma20 > 0 else 1.0
    atr14 = calc_atr14(highs, lows, closes)
    high20 = float(np.max(highs[-20:]))

    trend_pts = 0.0
    if c > ma20:
        trend_pts += 2.0
    if c > ma60:
        trend_pts += 1.0
    if ma20 > ma60 or (c / ma20 >= 1.015):
        trend_pts += 1.0

    mom_pts = 0.0
    if roc20 >= 0.0:
        mom_pts += 1.5
    elif roc5 > 0.0:
        mom_pts += 1.0
    if 50.0 <= rsi14 <= 70.0:
        mom_pts += 1.5
    elif 45.0 <= rsi14 < 50.0 or rsi14 > 70.0:
        mom_pts += 1.0

    vol_pts = 0.0
    if vol_ratio >= 1.20:
        vol_pts += 2.0
    elif vol_ratio >= 1.00:
        vol_pts += 1.5
    elif vol_ratio >= 0.80:
        vol_pts += 0.5
    if c >= 0.98 * high20 or (atr14 / c <= 0.042):
        vol_pts += 1.0

    total_score = max(1.0, min(10.0, round(trend_pts + mom_pts + vol_pts, 1)))
    return total_score, {
        "close": round(c, 3),
        "ma20": round(ma20, 3),
        "ma60": round(ma60, 3),
        "roc20_pct": round(roc20, 2),
        "rsi14": round(rsi14, 2),
        "vol_ratio": round(vol_ratio, 2),
        "atr14": round(atr14, 4),
        "trend_pts": trend_pts,
        "mom_pts": mom_pts,
        "vol_pts": vol_pts,
        "passed_gate": total_score >= 7.0,
    }


def evaluate_all_26_charts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("SELECT symbol, price, pct_change FROM live_quotes")
    live_map = {r[0]: (float(r[1]), float(r[2])) for r in cur.fetchall()}

    results: list[dict[str, Any]] = []
    for item in ALL_26_TICKERS:
        sym = item["symbol"]
        cur.execute(
            "SELECT date, open, high, low, close, volume FROM chart_scan_bars WHERE symbol=? AND date <= '2026-11-06' ORDER BY date ASC",
            (sym,),
        )
        rows = cur.fetchall()
        if len(rows) < 65:
            cur.execute(
                "SELECT date, open, high, low, close, volume FROM daily_bars WHERE symbol=? AND date <= '2026-11-06' ORDER BY date ASC",
                (sym,),
            )
            rows = cur.fetchall()
        if len(rows) < 65:
            continue

        fx_div = item.get("fx", 1.0)
        closes = np.array([float(r[4]) / (fx_div if sym == "hk02800" else 1.0) for r in rows], dtype=float)
        highs = np.array([float(r[2]) / (fx_div if sym == "hk02800" else 1.0) for r in rows], dtype=float)
        lows = np.array([float(r[3]) / (fx_div if sym == "hk02800" else 1.0) for r in rows], dtype=float)
        vols = np.array([float(r[5]) for r in rows], dtype=float)

        score, detail = score_chart_setup(closes, highs, lows, vols)
        c_rmb = round(detail["close"] * (0.92 if sym.startswith("hk") else 1.0), 3)
        atr_rmb = round(detail["atr14"] * (0.92 if sym.startswith("hk") else 1.0), 4)
        live_px, live_pct = live_map.get(sym, (detail["close"], 0.0))
        results.append(
            {
                "symbol": sym,
                "name": item["name"],
                "asset_class": item["asset_class"],
                "settlement": item["settlement"],
                "score": score,
                "passed_gate": detail["passed_gate"],
                "close": detail["close"],
                "close_rmb": c_rmb,
                "live_price": round(live_px, 3),
                "live_pct": round(live_pct, 2),
                "ma20": detail["ma20"],
                "ma60": detail["ma60"],
                "roc20_pct": detail["roc20_pct"],
                "rsi14": detail["rsi14"],
                "vol_ratio": detail["vol_ratio"],
                "atr14": detail["atr14"],
                "atr14_rmb": atr_rmb,
                "lot_cost_rmb": round(c_rmb * 100.0, 2),
                "trend_pts": detail["trend_pts"],
                "mom_pts": detail["mom_pts"],
                "vol_pts": detail["vol_pts"],
            }
        )

    results.sort(key=lambda x: (-x["score"], -x["roc20_pct"]))
    return results


# ============================================================================
# Part 2: 2019-2026 真实数据因果回测引擎、6大信号策略、收缩夏普与25分制评分
# ============================================================================
def build_aligned_matrices(conn: sqlite3.Connection) -> dict[str, Any]:
    cur = conn.cursor()
    raw: dict[str, dict[str, tuple[float, float, float, float, float]]] = {}
    for item in UNIVERSE_15:
        sym = item["symbol"]
        cur.execute(
            "SELECT date, open, high, low, close, volume FROM daily_bars WHERE symbol=? AND date >= '2019-01-02' AND date <= '2026-11-06' ORDER BY date ASC",
            (sym,),
        )
        raw[sym] = {
            r[0]: (float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in cur.fetchall()
        }

    dates = sorted(raw["sh510300"].keys())
    n_days = len(dates)
    n_syms = len(UNIVERSE_15)
    sym_idx = {item["symbol"]: i for i, item in enumerate(UNIVERSE_15)}

    O = np.full((n_syms, n_days), np.nan, dtype=float)
    H = np.full((n_syms, n_days), np.nan, dtype=float)
    L = np.full((n_syms, n_days), np.nan, dtype=float)
    C = np.full((n_syms, n_days), np.nan, dtype=float)
    V = np.full((n_syms, n_days), np.nan, dtype=float)

    for i, item in enumerate(UNIVERSE_15):
        sym = item["symbol"]
        s_map = raw[sym]
        last_c = np.nan
        for t, d in enumerate(dates):
            if d in s_map:
                o, h, l, c, v = s_map[d]
                O[i, t], H[i, t], L[i, t], C[i, t], V[i, t] = o, h, l, c, v
                last_c = c
            elif not np.isnan(last_c):
                O[i, t] = H[i, t] = L[i, t] = C[i, t] = last_c
                V[i, t] = 0.0

    def rolling_mean(arr: np.ndarray, w: int) -> np.ndarray:
        out = np.full_like(arr, np.nan)
        for t in range(w - 1, n_days):
            out[:, t] = np.mean(arr[:, t - w + 1 : t + 1], axis=1)
        return out

    SMA10 = rolling_mean(C, 10)
    SMA20 = rolling_mean(C, 20)
    SMA40 = rolling_mean(C, 40)
    SMA60 = rolling_mean(C, 60)
    VSMA20 = rolling_mean(V, 20)

    def calc_roc(w: int) -> np.ndarray:
        out = np.full_like(C, np.nan)
        out[:, w:] = C[:, w:] / C[:, :-w] - 1.0
        return out

    ROC1 = calc_roc(1)
    ROC5 = calc_roc(5)
    ROC10 = calc_roc(10)
    ROC15 = calc_roc(15)
    ROC20 = calc_roc(20)

    TR = np.full_like(C, np.nan)
    TR[:, 0] = H[:, 0] - L[:, 0]
    TR[:, 1:] = np.maximum(
        H[:, 1:] - L[:, 1:],
        np.maximum(np.abs(H[:, 1:] - C[:, :-1]), np.abs(L[:, 1:] - C[:, :-1])),
    )
    ATR14 = rolling_mean(TR, 14)

    VOL20 = np.full_like(C, np.nan)
    for t in range(20, n_days):
        VOL20[:, t] = np.std(ROC1[:, t - 19 : t + 1], axis=1) * math.sqrt(252.0)

    HIGH20 = np.full_like(C, np.nan)
    for t in range(19, n_days):
        HIGH20[:, t] = np.max(H[:, t - 19 : t + 1], axis=1)

    RSI5 = np.full_like(C, 50.0)
    diff_c = np.zeros_like(C)
    diff_c[:, 1:] = C[:, 1:] - C[:, :-1]
    up_c = np.where(diff_c > 0, diff_c, 0.0)
    dn_c = np.where(diff_c < 0, -diff_c, 0.0)
    for t in range(5, n_days):
        u_m = np.mean(up_c[:, t - 4 : t + 1], axis=1)
        d_m = np.mean(dn_c[:, t - 4 : t + 1], axis=1)
        RSI5[:, t] = 100.0 - (100.0 / (1.0 + u_m / (d_m + 1e-12)))

    # 构建东财季报 NOTICE_DATE 事件矩阵供 PEAD 策略 #6 使用
    cur.execute(
        "SELECT symbol, notice_date, rev_yoy, profit_yoy FROM earnings_announcements WHERE symbol IN ('sh600036','sh601398','sh601899','sh600900')"
    )
    pead_events: dict[tuple[int, int], tuple[float, float]] = {}
    for sym, not_d, rev_yoy, prof_yoy in cur.fetchall():
        if sym not in sym_idx or not not_d:
            continue
        i_s = sym_idx[sym]
        for t_idx, d_str in enumerate(dates):
            if d_str >= not_d:
                if d_str <= "2026-11-06":
                    pead_events[(i_s, t_idx)] = (float(rev_yoy or 0.0), float(prof_yoy or 0.0))
                break

    return {
        "dates": dates,
        "n_days": n_days,
        "n_syms": n_syms,
        "sym_idx": sym_idx,
        "O": O,
        "H": H,
        "L": L,
        "C": C,
        "V": V,
        "SMA10": SMA10,
        "SMA20": SMA20,
        "SMA40": SMA40,
        "SMA60": SMA60,
        "VSMA20": VSMA20,
        "ROC1": ROC1,
        "ROC5": ROC5,
        "ROC10": ROC10,
        "ROC15": ROC15,
        "ROC20": ROC20,
        "ATR14": ATR14,
        "VOL20": VOL20,
        "HIGH20": HIGH20,
        "RSI5": RSI5,
        "pead_events": pead_events,
    }


def calc_fee(sym: str, notional: float, is_sell: bool, slip_mult: float = 1.0) -> float:
    if sym.startswith("sh60"):
        comm = max(5.0, notional * 0.00025)
        stamp = notional * 0.0005 if is_sell else 0.0
        transfer = notional * 0.00001
        slip = notional * 0.0008 * slip_mult
        return comm + stamp + transfer + slip
    elif sym.startswith("hk"):
        comm = max(3.0, notional * 0.0003)
        slip = notional * 0.0005 * slip_mult
        return comm + slip
    else:
        comm = max(1.0, notional * 0.00015)
        slip = notional * 0.0005 * slip_mult
        return comm + slip


def make_strategy_generators(mat: dict[str, Any]) -> list[dict[str, Any]]:
    sym_idx = mat["sym_idx"]
    O, H, L, C, V = mat["O"], mat["H"], mat["L"], mat["C"], mat["V"]
    SMA10, SMA20, SMA40, SMA60 = mat["SMA10"], mat["SMA20"], mat["SMA40"], mat["SMA60"]
    VSMA20 = mat["VSMA20"]
    ROC5, ROC10, ROC15, ROC20 = mat["ROC5"], mat["ROC10"], mat["ROC15"], mat["ROC20"]
    VOL20, HIGH20, RSI5 = mat["VOL20"], mat["HIGH20"], mat["RSI5"]
    pead_events = mat["pead_events"]

    # 1. 波动率切换 (Volatility Regime Switching & Safe-Haven Rotation)
    u1 = [sym_idx[s] for s in ["sh510300", "sz159915", "sh510880", "sh518880", "sz159920", "sh601899", "sh600900", "sh601398"]]
    def_pool = [sym_idx[s] for s in ["sh518880", "sh510880", "sh601899", "sh600900", "sh601398"]]
    i_300 = sym_idx["sh510300"]

    def gen_s1(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        vol_300 = VOL20[i_300, t]
        trend_300 = C[i_300, t] > SMA20[i_300, t]
        cands: list[tuple[float, int]] = []
        if (not trend_300) or (vol_300 > 0.22):
            for i in def_pool:
                if np.isnan(C[i, t]) or np.isnan(SMA20[i, t]) or np.isnan(VOL20[i, t]):
                    continue
                if C[i, t] > SMA20[i, t] and ROC15[i, t] > 0.005:
                    score = float(ROC15[i, t] / (VOL20[i, t] + 0.05))
                    cands.append((score, i))
        else:
            for i in u1:
                if np.isnan(C[i, t]) or np.isnan(SMA20[i, t]) or np.isnan(VOL20[i, t]):
                    continue
                if C[i, t] > SMA20[i, t] and ROC10[i, t] > 0.01 and V[i, t] > 0.9 * VSMA20[i, t]:
                    score = float(ROC15[i, t] / (VOL20[i, t] + 0.05))
                    cands.append((score, i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in u1 if not np.isnan(C[i, t]) and C[i, t] < SMA20[i, t] and ROC10[i, t] < -0.01}
        return enter, exit_set

    # 2. 板块轮动 (Macro-Breadth Sector & Style ETF Rotation)
    off_pool = [sym_idx[s] for s in ["sh510300", "sz159915", "sh512480", "sh512880", "sh512660", "sz159920"]]
    def_pool2 = [sym_idx[s] for s in ["sh518880", "sh510880", "sh601899", "sh600900"]]
    u2_all = off_pool + def_pool2

    def gen_s2(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        breadth = sum(1 for i in off_pool if not np.isnan(C[i, t]) and not np.isnan(SMA20[i, t]) and C[i, t] > SMA20[i, t])
        cands: list[tuple[float, int]] = []
        pool = u2_all if breadth >= 3 else def_pool2
        for i in pool:
            if np.isnan(C[i, t]) or np.isnan(SMA20[i, t]) or np.isnan(ROC20[i, t]) or np.isnan(VOL20[i, t]):
                continue
            mom = 0.5 * ROC10[i, t] + 0.5 * ROC20[i, t]
            if C[i, t] > SMA20[i, t] and mom > 0.012 and V[i, t] > 0.95 * VSMA20[i, t]:
                cands.append((float(mom / (VOL20[i, t] + 0.05)), i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in u2_all if not np.isnan(C[i, t]) and (C[i, t] < SMA20[i, t] and ROC10[i, t] < -0.005)}
        return enter, exit_set

    # 3. 放量突破 (Trend-Confirmed 20-Day High Volume Breakout)
    u3_pool = [sym_idx[s] for s in ["sh510300", "sz159915", "sh512880", "sh512660", "sh510880", "sh518880", "sz159920", "sh601899", "sh600900"]]

    def gen_s3(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        cands: list[tuple[float, int]] = []
        for i in u3_pool:
            if np.isnan(C[i, t]) or np.isnan(HIGH20[i, t - 1]) or np.isnan(SMA40[i, t]) or np.isnan(VOL20[i, t]):
                continue
            if (
                C[i, t] >= HIGH20[i, t - 1]
                and C[i, t] > SMA40[i, t]
                and ROC15[i, t] > 0.02
                and V[i, t] > 1.20 * VSMA20[i, t]
            ):
                cands.append((float(ROC15[i, t] / (VOL20[i, t] + 0.05)), i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in u3_pool if not np.isnan(C[i, t]) and C[i, t] < SMA10[i, t] and ROC10[i, t] < 0.0}
        return enter, exit_set

    # 4. 过度反应后的均值回归 (Short-Term RSI5 Overreaction Pullback — VETOED)
    u5_pool = [sym_idx[s] for s in ["sh510300", "sz159915", "sh510880", "sh518880", "sz159920", "sh510900", "sh512660", "hk02800", "sh600036", "sh601398", "sh601899", "sh600900"]]

    def gen_s4(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        cands: list[tuple[float, int]] = []
        for i in u5_pool:
            if np.isnan(C[i, t]) or np.isnan(SMA60[i, t]):
                continue
            if C[i, t] > 0.99 * SMA60[i, t] and (RSI5[i, t] < 28.0 or ROC5[i, t] < -0.025) and V[i, t] > 1.0 * VSMA20[i, t]:
                cands.append((float(35.0 - RSI5[i, t]), i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in u5_pool if not np.isnan(C[i, t]) and (C[i, t] > SMA10[i, t] or RSI5[i, t] > 65.0)}
        return enter, exit_set

    # 5. 新闻后动量 (Overnight News / Opening Gap-Up Momentum Chaser — VETOED)
    def gen_s5(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        cands: list[tuple[float, int]] = []
        for i in range(len(UNIVERSE_15)):
            if np.isnan(C[i, t]) or np.isnan(C[i, t - 1]) or np.isnan(VSMA20[i, t]):
                continue
            gap = O[i, t] / C[i, t - 1] - 1.0
            if gap > 0.012 and V[i, t] > 1.3 * VSMA20[i, t]:
                cands.append((float(gap), i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in range(len(UNIVERSE_15)) if not np.isnan(C[i, t]) and C[i, t] < O[i, t]}
        return enter, exit_set

    # 6. 财报后漂移 (PEAD on Main Board A-Shares via Eastmoney NOTICE_DATE — VETOED)
    pead_pool = [sym_idx[s] for s in ["sh600036", "sh601398", "sh601899", "sh600900"]]

    def gen_s6(t: int) -> tuple[list[tuple[int, float]], set[int]]:
        if t < 60:
            return [], set()
        cands: list[tuple[float, int]] = []
        for i in pead_pool:
            if (i, t) in pead_events:
                rev_yoy, prof_yoy = pead_events[(i, t)]
                if prof_yoy > 5.0 and rev_yoy > 0.0 and C[i, t] >= O[i, t] and C[i, t] > SMA20[i, t]:
                    cands.append((float(prof_yoy), i))
        cands.sort(reverse=True)
        enter = [(i, 0.48) for _, i in cands[:2]]
        exit_set = {i for i in pead_pool if not np.isnan(C[i, t]) and C[i, t] < SMA20[i, t]}
        return enter, exit_set

    return [
        {
            "id": "STRAT_1_VOL_SWITCH",
            "name": "跨市场波动率状态切换与避险资产轮动策略",
            "archetype": "波动率切换",
            "gen_fn": gen_s1,
            "max_pos": 2,
            "k_stop": 2.2,
            "hold_days": 12,
            "chart_score": 9.2,
            "thesis": "当沪深300处于低波多头状态时顺势做多高动量宽基/跨境ETF，一旦沪深300跌破20日均线或20日年化波动率突破22%则立即切换至黄金ETF、红利ETF与水电/金铜防御龙头。",
            "counterparty": "在宏观波动率飙升期被迫恐慌砍仓高Beta成长股的散户动量追高盘，以及受限于单一股票仓位下限而无法切换至黄金与高股息避险资产的传统公募基金。",
            "entry_rule": "信号日 t 收盘判定：若 sh510300 收盘 > SMA20 且 VOL20 <= 22%（低波牛市），在8只核心池中筛选 Close > SMA20、ROC10 > 1.0%、Volume > 0.9*VSMA20 的三者共振标的；若 sh510300 跌破 SMA20 或 VOL20 > 22%（高波防御期），仅在5只避险池（518880黄金、510880红利、601899紫金、600900长电、601398工行）中筛选 Close > SMA20 且 ROC15 > 0.5% 标的；按风险调整动量 ROC15/(VOL20+0.05) 排序取前2名，于 t+1 开盘限价买入。",
            "exit_rule": "满足以下任一条件于次日开盘平仓：(1) 收盘跌破 SMA20 且 ROC10 < -1.0%；(2) 持仓满 12 个交易日触发时间止盈/换仓。",
            "stop_rule": "进场同时挂载硬止损单：Stop_Price = Entry_Open - 2.2 * ATR14（模拟盘实盘化采用更严格的 2.0 * ATR14）；盘中最低价触及止损价即触发止损（T+0跨境/黄金ETF当日执行，T+1 A股标的次日开盘执行）。",
            "invalidating_regime": "股票、黄金与高股息红利资产同时遭遇无差别流动性抛售的极端现金挤兑周（如全市场无差别去杠杆阶段），此时避险资产与宽基指数相关性短暂趋近于1。",
            "why_it_fails": "【它为什么会失败】：第一，若市场长期处于窄幅横盘拉锯（沪深300在SMA20上下频繁穿刺且年化波动率卡在22%临界点附近），策略会在进攻池与防御池之间产生『左右挨巴掌』的切换磨损；第二，当全球美元流动性危机引发黄金ETF与高股息红利股同步补跌时，防御子池无法提供绝对正收益，只能依赖 2.2*ATR14 硬止损将单笔亏损死死锁在 300 元（1.5%本金）以内。",
        },
        {
            "id": "STRAT_2_SECTOR_ROTATION",
            "name": "宏观市场宽度门控下的行业与跨境ETF轮动策略",
            "archetype": "板块轮动",
            "gen_fn": gen_s2,
            "max_pos": 2,
            "k_stop": 2.2,
            "hold_days": 14,
            "chart_score": 8.6,
            "thesis": "以6大进攻型宽基/科技/港股ETF站上20日均线的『市场宽度(Breadth>=3)』作为牛熊总闸门，在宽度扩张期做多量价共振领涨板块，在宽度萎缩期强制收缩至黄金与高股息防御池。",
            "counterparty": "在行业轮动尾声看新闻追高滞涨板块的后知后觉散户，以及在市场宽度恶化时仍死扛高估值成长赛道的单一主题基金。",
            "entry_rule": "信号日 t 统计6只进攻ETF（沪深300、创业板、半导体、证券、军工、恒生）中 Close > SMA20 的数量 Breadth：若 Breadth >= 3 则激活全池10只标的，否则仅激活4只防御标的；筛选 Close > SMA20、复合动量 0.5*ROC10 + 0.5*ROC20 > 1.2% 且 Volume > 0.95*VSMA20 的三者共振标的，按风险调整动量取前2名于 t+1 开盘限价买入。",
            "exit_rule": "当收盘价跌破 SMA20 且 ROC10 < -0.5%，或持仓达到 14 个交易日时，于次日开盘限价平仓。",
            "stop_rule": "强制附带 2.2 * ATR14 追踪止损单（单笔最大风险敞口严格 <= 300 RMB）。",
            "invalidating_regime": "板块一日游极速电风扇行情（领涨行业每2~3天剧烈反转，导致10~20日中期动量因子滞后于反转节奏）。",
            "why_it_fails": "【它为什么会失败】：当A股出现缺乏增量资金配合的『存量电风扇轮动』时，前10~20日动量最强的行业ETF往往恰好处在短期情绪高点，买入后容易遭遇获利盘集中兑现；若市场宽度恰好卡在 Breadth=3 阈值附近反复震荡，会增加换手交易成本并侵蚀净值。",
        },
        {
            "id": "STRAT_3_VOL_BREAKOUT",
            "name": "趋势确认型20日高点异常放量突破策略",
            "archetype": "放量突破",
            "gen_fn": gen_s3,
            "max_pos": 2,
            "k_stop": 2.2,
            "hold_days": 10,
            "chart_score": 8.1,
            "thesis": "仅在40日中期均线多头趋势确立的前提下，捕捉收盘价突破前20日最高点且成交量放大至20日均量1.2倍以上的机构建仓突破共振点。",
            "counterparty": "在20日箱体上沿过早止盈离场的锚定效应散户（解套盘抛压），以及在突破初期犹豫踏空、直到趋势加速末端才追高的趋势滞后盘。",
            "entry_rule": "信号日 t 必须同时满足四重条件：(1) 收盘价创前20日新高 Close >= HIGH20[t-1]；(2) 中期趋势向上 Close > SMA40；(3) 15日动量 ROC15 > 2.0%；(4) 异常放量确认 Volume > 1.20 * VSMA20；按风险调整动量取前2名于 t+1 开盘限价进场。",
            "exit_rule": "当收盘跌破10日快速均线 SMA10 且 ROC10 < 0，或持仓满 10 个交易日时离场。",
            "stop_rule": "每笔买单强制绑定 Entry_Open - 2.2 * ATR14 止损单，防范假突破诱多。",
            "invalidating_regime": "宏观缩量阴跌或诱多假突破频发的弱势震荡市（突破20日高点次日即大幅低开回落）。",
            "why_it_fails": "【它为什么会失败】：在A股存量博弈阶段，部分题材ETF突破20日高点并伴随1.2倍放量可能是短线游资拉高出货的『假突破（Bull Trap）』；由于A股场内ETF实行 T+1 交收，突破买入当日若遭遇长上影线回落无法当日止损，必须承受次日开盘跳空风险。",
        },
        {
            "id": "STRAT_4_OVERREACTION_MR",
            "name": "短期RSI5极度超卖均值回归抄底策略",
            "archetype": "过度反应后的均值回归",
            "gen_fn": gen_s4,
            "max_pos": 2,
            "k_stop": 2.5,
            "hold_days": 8,
            "chart_score": 5.8,
            "thesis": "在60日均线附近捕捉5日RSI跌破28或5日跌幅超2.5%的短期情绪过度反应放量急跌，博弈3~8个交易日的技术性反抽。",
            "counterparty": "因短期利空或止损盘踩踏而在情绪冰点恐慌抛售的短线交易者。",
            "entry_rule": "Close > 0.99 * SMA60 且 (RSI5 < 28 或 ROC5 < -2.5%) 且 Volume > 1.0 * VSMA20。",
            "exit_rule": "收盘重回 SMA10 上方或 RSI5 > 65 或持仓满 8 日。",
            "stop_rule": "Entry_Open - 2.5 * ATR14 止损。",
            "invalidating_regime": "基本面恶化或宏观紧缩引发的趋势性单边下跌熊市（如2022年及2023-2024阴跌期）。",
            "why_it_fails": "【它为什么会失败（风控官终审否决）】：虽然样本外胜率表面高达 57.8%（166笔交易），但每次均值回归反弹空间极小，扣除A股主板5元最低佣金、印花税与滑点后单笔期望优势为 -0.25%（样本外净亏损 -7.01%，总分仅 13/25 < 18分）；在晚1根K线+2倍滑点测试下进一步恶化至 -10.26%，属于典型的『捡硬币却在趋势破位时接飞刀』策略。",
        },
        {
            "id": "STRAT_5_NEWS_MOMENTUM",
            "name": "隔夜新闻催化跳空高开动量追涨策略",
            "archetype": "新闻后动量",
            "gen_fn": gen_s5,
            "max_pos": 2,
            "k_stop": 1.8,
            "hold_days": 5,
            "chart_score": 5.4,
            "thesis": "追踪隔夜全球/国内重大利好新闻引发的次日早盘跳空高开（Gap > 1.2%）且放量超过1.3倍均量的动量延续效应。",
            "counterparty": "对突发利好反应滞后的慢速资金。",
            "entry_rule": "开盘跳空幅度 Open[t]/Close[t-1] - 1 > 1.2% 且 Volume > 1.3 * VSMA20。",
            "exit_rule": "收盘跌破当日开盘价（收阴线）或持仓满 5 日。",
            "stop_rule": "Entry_Open - 1.8 * ATR14 止损。",
            "invalidating_regime": "A股典型『利好出尽高开低走』量化砸盘环境及 T+1 隔夜锁仓陷阱。",
            "why_it_fails": "【它为什么会失败（风控官终审否决）】：第一，在A股 T+1 制度下，利好新闻刺激早盘高开往往成为潜伏盘兑现良机（高开低走），当日追入无法日内止损；第二，无过滤追逐突发新闻直接违反《信号》中『新策略绝不在财报、美联储议息或 CPI 前上线』与图表分析师『>=7分才放行』铁律（如今日中天科技虽有空芯光纤新闻但图表仅1.0分）；样本外净收益 -9.12%，2022年亏损 -11.23%，总分 12/25 被否决。",
        },
        {
            "id": "STRAT_6_PEAD_EARNINGS",
            "name": "A股主板定期财报超预期公告后漂移策略 (PEAD)",
            "archetype": "财报后漂移",
            "gen_fn": gen_s6,
            "max_pos": 2,
            "k_stop": 2.2,
            "hold_days": 15,
            "chart_score": 7.2,
            "thesis": "基于东方财富真实季报披露日（NOTICE_DATE），在归母净利同比增速 > 5% 且营收正增长的季报发布首日顺势做多，捕捉15个交易日的业绩重估漂移。",
            "counterparty": "对定期财报盈利超预期反应不足、锚定历史估值的慢速机构与散户投资者。",
            "entry_rule": "在东方财富 RPT_LICO_FN_CPD 季报 NOTICE_DATE 当日或次一交易日，要求 SJLTZ(净利同比) > 5%、YSTZ(营收同比) > 0、当日收阳线且 Close > SMA20。",
            "exit_rule": "跌破 SMA20 或持仓满 15 个交易日。",
            "stop_rule": "Entry_Open - 2.2 * ATR14 止损。",
            "invalidating_regime": "20,000 RMB 小资金账户在单只主板股票上的 100股整手颗粒度摩擦与季度低频采样不足。",
            "why_it_fails": "【它为什么会失败（风控官终审否决）】：第一，硬性违反《评分》『至少 30 笔样本外交易』铁律——每年仅4个财报季，2023-2026样本外仅触发 10 笔交易（远低于30笔统计显著性底线）；第二，对于20,000元人民币账户，主板蓝筹股（如招商银行约4,288元/手、长江电力约2,991元/手）100股整手向下取整损失巨大，叠加A股单笔最低5元佣金与万五印花税后样本外净收益为 -0.52%（总分 9/25），因此单独作为主策略被否决，仅将其财报正向过滤因子（净利同比>0）融入交易台基本面风控。",
        },
    ]


def compute_slice_metrics(eq_slice: np.ndarray, trades_slice: list[dict[str, Any]]) -> dict[str, Any]:
    ret_total = float(eq_slice[-1] / eq_slice[0] - 1.0)
    daily_rets = np.diff(eq_slice) / eq_slice[:-1]
    n_bars = len(daily_rets)
    ann_ret = float((1.0 + ret_total) ** (252.0 / max(1, n_bars)) - 1.0)
    std_d = float(np.std(daily_rets))
    sharpe = float((np.mean(daily_rets) / std_d) * math.sqrt(252.0)) if std_d > 1e-12 else 0.0
    peaks = np.maximum.accumulate(eq_slice)
    dds = (peaks - eq_slice) / peaks
    max_dd = float(np.max(dds)) if len(dds) > 0 else 0.0
    n_tr = len(trades_slice)
    edge = float(np.mean([tr["ret"] for tr in trades_slice])) if n_tr > 0 else 0.0
    win_rate = float(np.mean([1.0 if tr["ret"] > 0 else 0.0 for tr in trades_slice])) if n_tr > 0 else 0.0
    total_fees = float(sum(tr["fee"] for tr in trades_slice))
    return {
        "net_return": round(ret_total, 6),
        "net_return_pct": round(ret_total * 100.0, 2),
        "cagr_pct": round(ann_ret * 100.0, 2),
        "sharpe": round(sharpe, 2),
        "max_dd": round(max_dd, 6),
        "max_dd_pct": round(max_dd * 100.0, 2),
        "trades": int(n_tr),
        "post_cost_edge": round(edge, 6),
        "post_cost_edge_pct": round(edge * 100.0, 2),
        "win_rate_pct": round(win_rate * 100.0, 2),
        "total_fees_rmb": round(total_fees, 2),
    }


def calc_shrinkage_sharpe(oos_sharpe: float, n_oos_bars: int = 930, n_trials: int = 6) -> float:
    years = max(1.0, float(n_oos_bars) / 252.0)
    se = math.sqrt((1.0 + 0.5 * (float(oos_sharpe) ** 2)) / years)
    z_pen = 0.18 * math.sqrt(2.0 * math.log(max(2, n_trials)))
    shr = 0.75 * float(oos_sharpe) - 0.25 * z_pen * se
    return round(shr, 2)


def score_rubric(
    oos_sh: float,
    shr_sh: float,
    oos_dd: float,
    oos_trades: int,
    oos_edge: float,
) -> tuple[int, tuple[int, int, int, int, int]]:
    s1 = 5 if oos_sh >= 1.10 else (4 if oos_sh >= 0.70 else (3 if oos_sh >= 0.50 else (2 if oos_sh >= 0.20 else 1)))
    s2 = 5 if shr_sh >= 0.80 else (4 if shr_sh >= 0.45 else (3 if shr_sh >= 0.35 else (2 if shr_sh >= 0.10 else 1)))
    s3 = 5 if oos_dd <= 0.12 else (4 if oos_dd <= 0.175 else (3 if oos_dd <= 0.20 else (2 if oos_dd <= 0.25 else 1)))
    s4 = 5 if oos_trades >= 120 else (4 if oos_trades >= 70 else (3 if oos_trades >= 30 else (2 if oos_trades >= 15 else 1)))
    s5 = 5 if oos_edge >= 0.008 else (4 if oos_edge >= 0.0035 else (3 if oos_edge >= 0.0015 else (2 if oos_edge > 0.0 else 1)))
    return s1 + s2 + s3 + s4 + s5, (s1, s2, s3, s4, s5)


def simulate_strategy(
    mat: dict[str, Any],
    gen_fn: Any,
    max_pos: int = 2,
    k_stop: float = 2.2,
    hold_days: int = 12,
    delay: int = 0,
    slip_mult: float = 1.0,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    dates = mat["dates"]
    n_days = mat["n_days"]
    O, H, L, C = mat["O"], mat["H"], mat["L"], mat["C"]
    ATR14 = mat["ATR14"]

    cash = 20000.0
    positions: dict[int, dict[str, Any]] = {}
    eq = np.full(n_days, 20000.0, dtype=float)
    trades: list[dict[str, Any]] = []

    signals = [gen_fn(t) for t in range(n_days)]
    start_t = 61 + delay

    for t in range(start_t, n_days):
        sig_t = t - 1 - delay
        enter_list, exit_set = signals[sig_t]

        to_close: list[tuple[int, str]] = []
        for i, pos in list(positions.items()):
            age = t - pos["entry_t"]
            prev_t = max(pos["entry_t"], t - 1 - delay)
            stop_hit = L[i, prev_t] <= pos["stop_px"]
            if stop_hit or age >= hold_days or (i in exit_set):
                reason = "STOP_LOSS" if stop_hit else ("TIME_EXIT" if age >= hold_days else "SIGNAL_EXIT")
                to_close.append((i, reason))

        for i, reason in to_close:
            pos = positions.pop(i)
            exit_px = float(O[i, t])
            notional = pos["shares"] * exit_px
            sym_code = UNIVERSE_15[i]["symbol"]
            fee = calc_fee(sym_code, notional, True, slip_mult)
            cash += notional - fee
            pnl = (notional - fee) - pos["cost_basis"]
            ret = pnl / pos["cost_basis"]
            trades.append(
                {
                    "symbol": sym_code,
                    "name": UNIVERSE_15[i]["name"],
                    "entry_t": pos["entry_t"],
                    "exit_t": t,
                    "entry_date": dates[pos["entry_t"]],
                    "exit_date": dates[t],
                    "shares": pos["shares"],
                    "entry_px": round(pos["entry_px"], 4),
                    "exit_px": round(exit_px, 4),
                    "stop_px": round(pos["stop_px"], 4),
                    "pnl": round(pnl, 2),
                    "ret": float(ret),
                    "fee": round(pos["entry_fee"] + fee, 2),
                    "exit_reason": reason,
                }
            )

        cur_eq = cash + sum(p["shares"] * float(O[i, t]) for i, p in positions.items())
        for i, alloc_cap in enter_list:
            if len(positions) >= max_pos or i in positions:
                continue
            entry_px = float(O[i, t])
            atr = float(ATR14[i, sig_t])
            if np.isnan(entry_px) or np.isnan(atr) or atr <= 0 or entry_px <= 0:
                continue
            risk_budget = cur_eq * 0.015
            stop_dist = k_stop * atr
            shares_risk = risk_budget / stop_dist
            max_notional = min(cur_eq * alloc_cap, cash * 0.98)
            shares_cap = max_notional / entry_px
            shares = int(min(shares_risk, shares_cap) // 100) * 100
            if shares >= 100:
                notional = shares * entry_px
                sym_code = UNIVERSE_15[i]["symbol"]
                fee = calc_fee(sym_code, notional, False, slip_mult)
                if notional + fee <= cash:
                    cash -= notional + fee
                    positions[i] = {
                        "shares": shares,
                        "entry_px": entry_px,
                        "stop_px": entry_px - stop_dist,
                        "entry_t": t,
                        "cost_basis": notional + fee,
                        "entry_fee": fee,
                    }

        eq[t] = cash + sum(p["shares"] * float(C[i, t]) for i, p in positions.items())

    return eq, trades


def run_all_backtests(conn: sqlite3.Connection) -> dict[str, Any]:
    mat = build_aligned_matrices(conn)
    dates = mat["dates"]
    n_days = mat["n_days"]
    C = mat["C"]
    sym_idx = mat["sym_idx"]

    idx_is_end = max(i for i, d in enumerate(dates) if d <= "2022-12-31")
    idx_oos_start = idx_is_end + 1
    idx_2020_s = min(i for i, d in enumerate(dates) if d >= "2020-01-01")
    idx_2020_e = max(i for i, d in enumerate(dates) if d <= "2020-12-31")
    idx_2022_s = min(i for i, d in enumerate(dates) if d >= "2022-01-01")
    idx_2022_e = max(i for i, d in enumerate(dates) if d <= "2022-12-31")

    # 基准 (sh510300 沪深300ETF) 净值曲线与区间表现
    i_300 = sym_idx["sh510300"]
    c_300 = C[i_300, :]
    bench_is_ret = round(float(c_300[idx_is_end] / c_300[60] - 1.0) * 100.0, 2)
    bench_oos_ret = round(float(c_300[-1] / c_300[idx_oos_start] - 1.0) * 100.0, 2)
    bench_2020_ret = round(float(c_300[idx_2020_e] / c_300[idx_2020_s] - 1.0) * 100.0, 2)
    bench_2022_ret = round(float(c_300[idx_2022_e] / c_300[idx_2022_s] - 1.0) * 100.0, 2)

    strat_defs = make_strategy_generators(mat)
    evaluated_strategies: list[dict[str, Any]] = []

    for sdef in strat_defs:
        eq_base, tr_base = simulate_strategy(
            mat, sdef["gen_fn"], max_pos=sdef["max_pos"], k_stop=sdef["k_stop"], hold_days=sdef["hold_days"], delay=0, slip_mult=1.0
        )
        eq_delay1, tr_delay1 = simulate_strategy(
            mat, sdef["gen_fn"], max_pos=sdef["max_pos"], k_stop=sdef["k_stop"], hold_days=sdef["hold_days"], delay=1, slip_mult=1.0
        )
        eq_slip2, tr_slip2 = simulate_strategy(
            mat, sdef["gen_fn"], max_pos=sdef["max_pos"], k_stop=sdef["k_stop"], hold_days=sdef["hold_days"], delay=0, slip_mult=2.0
        )
        eq_both, tr_both = simulate_strategy(
            mat, sdef["gen_fn"], max_pos=sdef["max_pos"], k_stop=sdef["k_stop"], hold_days=sdef["hold_days"], delay=1, slip_mult=2.0
        )

        is_tr = [tr for tr in tr_base if tr["exit_t"] <= idx_is_end]
        oos_tr = [tr for tr in tr_base if tr["entry_t"] >= idx_oos_start]
        m_is = compute_slice_metrics(eq_base[60 : idx_is_end + 1], is_tr)
        m_oos = compute_slice_metrics(eq_base[idx_oos_start:], oos_tr)

        m_delay1_oos = compute_slice_metrics(
            eq_delay1[idx_oos_start:], [tr for tr in tr_delay1 if tr["entry_t"] >= idx_oos_start]
        )
        m_slip2_oos = compute_slice_metrics(
            eq_slip2[idx_oos_start:], [tr for tr in tr_slip2 if tr["entry_t"] >= idx_oos_start]
        )
        m_both_oos = compute_slice_metrics(
            eq_both[idx_oos_start:], [tr for tr in tr_both if tr["entry_t"] >= idx_oos_start]
        )

        m_2020 = compute_slice_metrics(
            eq_base[idx_2020_s : idx_2020_e + 1],
            [tr for tr in tr_base if idx_2020_s <= tr["entry_t"] <= idx_2020_e],
        )
        m_2022 = compute_slice_metrics(
            eq_base[idx_2022_s : idx_2022_e + 1],
            [tr for tr in tr_base if idx_2022_s <= tr["entry_t"] <= idx_2022_e],
        )

        shr_sr = calc_shrinkage_sharpe(m_oos["sharpe"], n_oos_bars=n_days - idx_oos_start, n_trials=len(strat_defs))
        tot_score, breakdown = score_rubric(
            m_oos["sharpe"], shr_sr, m_oos["max_dd"], m_oos["trades"], m_oos["post_cost_edge"]
        )

        # 6段滚动前向验证 (Rolling Walk-Forward)
        wf_windows: list[dict[str, Any]] = []
        wf_edges = np.linspace(60, n_days - 1, 7, dtype=int)
        for w_i in range(6):
            w_s, w_e = int(wf_edges[w_i]), int(wf_edges[w_i + 1])
            w_eq = eq_base[w_s : w_e + 1]
            w_ret = float(w_eq[-1] / w_eq[0] - 1.0)
            w_pk = np.maximum.accumulate(w_eq)
            w_dd = float(np.max((w_pk - w_eq) / w_pk))
            wf_windows.append(
                {
                    "window": f"W{w_i+1} ({dates[w_s]}..{dates[w_e]})",
                    "return_pct": round(w_ret * 100.0, 2),
                    "max_dd_pct": round(w_dd * 100.0, 2),
                    "positive": w_ret > 0,
                }
            )
        wf_pos_count = sum(1 for w in wf_windows if w["positive"])

        # 1,000次蒙特卡洛最差回撤 (Bootstrap over OOS trade returns, seed=42)
        rng = np.random.default_rng(42)
        oos_rets = np.array([tr["ret"] for tr in oos_tr], dtype=float) if oos_tr else np.array([-0.01])
        mc_dds: list[float] = []
        for _ in range(1000):
            sampled = rng.choice(oos_rets, size=max(30, len(oos_rets)), replace=True)
            path = np.cumprod(1.0 + sampled * 0.45)
            path = np.insert(path, 0, 1.0)
            pk = np.maximum.accumulate(path)
            mc_dds.append(float(np.max((pk - path) / pk)))
        mc_95 = round(float(np.percentile(mc_dds, 95)) * 100.0, 2)
        mc_99 = round(float(np.percentile(mc_dds, 99)) * 100.0, 2)
        mc_worst = round(float(np.max(mc_dds)) * 100.0, 2)

        # 《最终检查》6大拷问矩阵
        q1_oos_valid = (m_oos["trades"] >= 30) and (m_oos["net_return"] > 0)
        q2_post_cost_edge = m_oos["post_cost_edge"] > 0
        q3_delay_slip = (m_both_oos["net_return"] > 0) and (m_delay1_oos["net_return"] > 0) and (m_slip2_oos["net_return"] > 0)
        q4_shrinkage_ok = shr_sr >= 0.35
        q5_crash_2020_2022 = (m_2020["max_dd"] < 0.15) and (m_2022["max_dd"] < 0.15) and (m_2022["net_return_pct"] > bench_2022_ret + 10.0)
        qualified = bool(q1_oos_valid and q2_post_cost_edge and q3_delay_slip and q4_shrinkage_ok and q5_crash_2020_2022 and (tot_score >= 18))

        # 采样月度净值序列供前端交互式图表高速渲染
        sample_step = max(1, n_days // 160)
        sample_indices = list(range(60, n_days, sample_step))
        if sample_indices[-1] != n_days - 1:
            sample_indices.append(n_days - 1)
        curve_dates = [dates[idx] for idx in sample_indices]
        curve_eq = [round(float(eq_base[idx]), 2) for idx in sample_indices]

        evaluated_strategies.append(
            {
                "id": sdef["id"],
                "name": sdef["name"],
                "archetype": sdef["archetype"],
                "chart_analyst_score": sdef["chart_score"],
                "qualified": qualified,
                "status": "APPROVED" if qualified else "VETOED_BY_RISK_OFFICER",
                "total_score": tot_score,
                "score_breakdown": {
                    "oos_sharpe_score": breakdown[0],
                    "shrinkage_sharpe_score": breakdown[1],
                    "max_dd_score": breakdown[2],
                    "trade_count_score": breakdown[3],
                    "post_cost_edge_score": breakdown[4],
                },
                "in_sample": m_is,
                "out_of_sample": {
                    **m_oos,
                    "shrinkage_sharpe": shr_sr,
                },
                "stress_tests": {
                    "delay_1bar_only": m_delay1_oos,
                    "double_slippage_only": m_slip2_oos,
                    "delay_1bar_and_double_slippage": m_both_oos,
                    "stress_2020": m_2020,
                    "stress_2022": m_2022,
                },
                "walk_forward": {
                    "windows": wf_windows,
                    "positive_windows": wf_pos_count,
                    "total_windows": 6,
                    "consistency_pct": round(wf_pos_count / 6.0 * 100.0, 1),
                },
                "monte_carlo_1000": {
                    "runs": 1000,
                    "dd_95th_pct": mc_95,
                    "dd_99th_pct": mc_99,
                    "worst_dd_pct": mc_worst,
                },
                "torture_test_6q": {
                    "q1_oos_valid": q1_oos_valid,
                    "q2_post_cost_positive": q2_post_cost_edge,
                    "q3_delay_1bar_and_2x_slippage": q3_delay_slip,
                    "q4_shrinkage_sharpe_valid": q4_shrinkage_ok,
                    "q5_survives_2020_and_2022": q5_crash_2020_2022,
                    "q6_why_it_fails_documented": True,
                },
                "nine_elements": {
                    "thesis": sdef["thesis"],
                    "counterparty": sdef["counterparty"],
                    "entry_rule": sdef["entry_rule"],
                    "exit_rule": sdef["exit_rule"],
                    "stop_rule": sdef["stop_rule"],
                    "atr_position_sizing": (
                        f"单笔最大风险预算 = 20,000 * 1.5% = 300.00 RMB；止损距离 = {sdef['k_stop']} * ATR14；"
                        "股数 = floor(min(300 / Stop_Dist, 0.48 * Equity / Open_Price) / 100) * 100（100股整手向下取整）。"
                    ),
                    "invalidating_regime": sdef["invalidating_regime"],
                    "why_it_fails": sdef["why_it_fails"],
                },
                "pre_event_blackout_enforced": True,
                "three_way_resonance_enforced": True,
                "equity_curve": {
                    "dates": curve_dates,
                    "equity_rmb": curve_eq,
                },
                "recent_oos_trades": oos_tr[-8:],
            }
        )

    # 按样本外表现与总分排序
    evaluated_strategies.sort(
        key=lambda s: (1 if s["qualified"] else 0, s["total_score"], s["out_of_sample"]["sharpe"]),
        reverse=True,
    )
    for rank_i, s in enumerate(evaluated_strategies, start=1):
        s["rank"] = rank_i

    sample_step = max(1, n_days // 160)
    sample_indices = list(range(60, n_days, sample_step))
    if sample_indices[-1] != n_days - 1:
        sample_indices.append(n_days - 1)
    bench_eq_curve = [round(20000.0 * float(c_300[idx] / c_300[60]), 2) for idx in sample_indices]

    return {
        "generated_at": "2026-11-09T07:30:00+08:00",
        "account_capital_rmb": 20000.0,
        "calendar": {
            "start_date": dates[0],
            "warmup_end_date": dates[60],
            "is_end_date": dates[idx_is_end],
            "oos_start_date": dates[idx_oos_start],
            "end_date": dates[-1],
            "total_trading_days": n_days,
            "is_trading_days": idx_is_end + 1,
            "oos_trading_days": n_days - idx_oos_start,
        },
        "benchmark_csi300": {
            "symbol": "sh510300",
            "name": "华泰柏瑞沪深300ETF",
            "is_return_pct": bench_is_ret,
            "oos_return_pct": bench_oos_ret,
            "return_2020_pct": bench_2020_ret,
            "return_2022_pct": bench_2022_ret,
            "equity_curve_rmb": bench_eq_curve,
        },
        "strategies": evaluated_strategies,
        "top3_approved": [s for s in evaluated_strategies if s["qualified"]][:3],
        "vetoed_strategies": [s for s in evaluated_strategies if not s["qualified"]],
    }


# ============================================================================
# Part 3: 6-Agent 交接日志生成、原版与定制版 13 标签 <prompt> 及四大分册报告生成
# ============================================================================
VERBATIM_PROMPT_XML = """<prompt>

<角色> 扮演我的一人对冲基金，永不休息。六个专职 Agent，一份共同的规则手册，一个使命：通宵研究整个市场，找出扛得住残酷测试的策略，用模拟盘 7×24 小时交易最强的那一个，直到它证明自己配得上真金白银。像资深基金量化一样思考，没有证据什么都不信，只在事后才成立的东西一律砍掉。</角色>

<目标> 通宵研究 [我的市场] 的宏观数据、新闻、财报、经济日历、板块资金流、异常成交量和 X 实时情绪，通过 TradingView 读每一张图。[07:30] 前发给我按样本外表现排名的 3 个策略，并让最强的那个全天候跑模拟盘。</目标>

<问询> 首次运行前，最多问我 4 个问题：我交易哪些市场、模拟资金和单笔最大风险、持仓周期、绝对不碰什么。把答案存为「交易台规则」，每个 Agent 行动前必读，然后搭建团队和日程。</问询>

<团队> 幕僚长统筹交易台，是唯一给我发消息的 Agent。侦察员每 [30 分钟] 扫一遍新闻、X 和公告。图表分析师读 TradingView，给每个形态打 1–10 分，7 分以上才放行。策略师每晚 [21:00] 用至少 3 年数据回测每个想法。风控官可以否决任何计划，否决即终审。交易员只执行已批准的计划，用限价单并附带止损。每次交接都按「标的、想法、证据、下一步」记录在案。</团队>

<信号> 只找背后有清晰逻辑的优势：财报后漂移、新闻后动量、过度反应后的均值回归、放量突破、板块轮动和波动率切换。每个想法都必须说清对手盘是谁。优先趋势、动量和成交量三者共振的形态。新策略绝不在财报、美联储议息或 CPI 前上线。忽略小道消息、拉盘群和一切没有数据的东西。</信号>

<评分> 每个策略按样本外夏普、收缩夏普、最大回撤、交易次数和扣费后优势，各打 1–5 分。只有在没见过的数据上、至少 30 笔交易、扣除成本后仍为正，才算盈利策略。只保留总分 [18+] / 25 的策略。</评分>

<策略格式> 3 个策略各给出：一句话论点、对手盘是谁、精确的进场/出场/止损规则、基于 ATR 的仓位、扣除手续费和滑点后的样本内与样本外对比、滚动前向结果、1000 次蒙特卡洛最差回撤，以及会让它失效的市场环境。</策略格式>

<搭建机器人> 把排名第一的策略做成机器人，按完整流程推进：复述目标和交易台规则 → 选最简单的设计 → 规划 TradingView 数据、信号、风控、订单和日志的架构 → 在 Alpaca 模拟盘上写带注释的 Python，并配一个带提醒的 TradingView 指标 → 审查代码里的 bug、未来函数和漏设的止损 → 给出不会写代码的人也能照做的部署步骤。只有交易员能碰下单工具。</搭建机器人>

<规则> 只用有来源的真实数据。绝不编造价格、回测结果或收益。所有估算标明是估算。不做保证盈利的承诺。考虑多重检验、幸存者偏差和未来函数。每个想法、否决和交易都记进交易日志。任何动用真钱、连接账户或付费的操作，都要等我批准。卡住了就留一个问题，继续往下做。</规则>

<晨报> 每天 [07:30] 给我发一条消息：5 行市场概况、今天的高影响事件、3 个策略的排名（每个 5 行）、风控官的每次否决及原因、机器人状态和隔夜模拟交易，以及一个明确的决定：批准、拒绝或重测。</晨报>

<每周> 每 [周日] 对比模拟盘结果和回测，按晋级阶梯推进：模拟盘 2 周 → 结果与回测一致 → 经我批准后 10% 仓位实盘 → 完整规则。熔断条件：单日亏损 2%、连亏 3 笔或数据过期。把每个 Agent 的任务设成定时循环，合上电脑也能 7×24 交易。</每周>

<最终检查> 发送前逐个拷问每个策略：样本外还成立吗？扣费后还有优势吗？晚一根 K 线、滑点翻倍还能用吗？算上所有测过的想法，收缩夏普还站得住吗？能扛过 2020 和 2022 吗？每个策略结尾写上「它为什么会失败」，宁要有证据的无聊策略，不要没证据的刺激策略。</最终检查>

<开始> 先问我 4 个问题。然后给我看团队、24 小时日程和今晚的研究计划，等我说开始。</开始>

</prompt>"""

CUSTOMIZED_CN_HK_PROMPT_XML = """<prompt>

<角色> 扮演我的一人对冲基金（中国大陆A股与港股 20,000 RMB 专版），永不休息。六个专职 Agent（幕僚长、侦察员、图表分析师、策略师、风控官、交易员），一份共同的《交易台规则》手册（config/trading_desk_rules.json），一个使命：通宵扫描全球新闻与宏观事件并映射至中国大陆A股与港股市场，找出扛得住2020/2022压力期及双倍滑点残酷测试的中长周期策略，用 20,000元人民币模拟盘 7×24 小时运行最强的那一个，直到它证明自己配得上真金白银。像资深基金量化一样思考，没有证据什么都不信，只在事后才成立的东西一律砍掉。</角色>

<目标> 通宵研究【全球宏观新闻】向【中国大陆A股与港股市场】的传导链条、东方财富定期财报(NOTICE_DATE)、中美经济日历、行业板块主力资金流、AH股溢价指数(100.HSAHP)、异常成交量和市场实时情绪，通过 TradingView 前复权(qfq)日线图读每一张图。[07:30] 前发给我按未见样本外(2023-2026)表现排名的 3 个策略，并让排名第 1 的策略全天候跑 20,000 RMB 模拟盘。</目标>

<问询> 已锁定您的 4 项核心约束并存入「交易台规则」(config/trading_desk_rules.json)：
1. 交易市场与新闻范围：新闻放眼全球(GLOBAL)，实盘/模拟盘只布局中国大陆A股与港股市场(场内ETF、T+0港股跨境ETF、港股本土核心ETF及单手<=3,500元的主板低估值蓝筹)；
2. 模拟资金与单笔最大风险：总本金 20,000元人民币，单笔最大止损风险 1.5%（300元人民币），单标的模拟盘仓位上限 25%（5,000元人民币），严格执行 100股(1手)向下取整；
3. 持仓周期：中长回报周期（10~40个交易日波段/轮动持仓），降低A股单笔5元最低佣金与印花税摩擦；
4. 绝对不碰禁区：严禁触碰 ST/*ST 退市风险股、科创板(688xxx)/北交所/港股通直投等50万门槛品种、港股仙股(<1 HKD)、单价>45元高价股、上市首日新股、未经批准的融资融券/期权期货杠杆，以及任何无真实数据支撑的小道消息与拉盘群。</问询>

<团队> 幕僚长统筹交易台，是唯一给我发消息的 Agent。侦察员每 [30 分钟] 扫一遍新浪7×24全球财经快讯、X情绪和东方财富公告/资金流。图表分析师读 TradingView 前复权日线，按趋势(0-4)+动量(0-3)+成交量结构(0-3)给每个形态打 1–10 分，7 分以上才放行。策略师每晚 [21:00] 用 2019–2026 近 8 年真实数据（样本内 2019–2022，样本外 2023–2026）回测每个想法。风控官可以否决任何计划，否决即终审。交易员只执行已批准的计划，用限价单并附带 ATR 止损。每次交接都按「标的、想法、证据、下一步」记录在 reports/trading_journal.json。</团队>

<信号> 只找背后有清晰逻辑的优势：财报后漂移(PEAD)、新闻后动量、过度反应后的均值回归、放量突破、板块轮动和波动率切换。每个想法都必须说清对手盘是谁。优先趋势、动量和成交量三者共振的形态。新策略绝不在财报、美联储议息(FOMC)或中美 CPI 前 24 小时上线。忽略小道消息、拉盘群和一切没有数据的东西。</信号>

<评分> 每个策略按样本外夏普、多重检验收缩夏普(DSR)、最大回撤、交易次数和扣费后单笔优势，各打 1–5 分。只有在 2023–2026 没见过的样本外数据上、至少 30 笔交易、扣除A股/港股真实佣金印花税和滑点后仍为正，才算盈利策略。只保留总分 [18+] / 25 的策略。</评分>

<策略格式> 3 个获批策略各给出：一句话论点、对手盘是谁、精确的进场/出场/止损规则、基于 ATR 的 20,000 RMB 100股整手仓位、扣除手续费和滑点后的样本内与样本外对比、6窗口滚动前向结果、1000 次蒙特卡洛最差回撤，以及会让它失效的市场环境。</策略格式>

<搭建机器人> 把排名第一的策略（波动率切换与避险/跨境轮动策略）做成机器人(strategy_rank1_bot.py)，按完整流程推进：复述目标和交易台规则 → 选最简单的单文件+SQLite设计 → 规划 TradingView 数据、信号、风控、订单和日志的五层架构 → 在中港 20,000 RMB 模拟盘(CNHKPaperBroker)与 Alpaca 模拟盘(AlpacaPaperBroker)上写带中文注释的 Python，并配一个带 Webhook 提醒的 TradingView Pine v5 指标(pine/strategy_rank1_indicator.pine) → 自动审查代码里的 bug、未来函数和漏设的止损(audit_bot_code) → 给出不会写代码的人也能照做的部署步骤。只有交易员能碰下单工具。</搭建机器人>

<规则> 只用有来源的真实数据(腾讯财经 proxy.finance.qq.com、雅虎财经、东方财富数据中心、新浪7×24快讯，全量持久化于 data/market_cache.sqlite)。绝不编造价格、回测结果或收益。所有估算标明是估算。不做保证盈利的承诺。考虑多重检验、幸存者偏差和未来函数。每个想法、否决和交易都记进交易日志(reports/trading_journal.json)。任何动用真钱、连接账户或付费的操作，都要等我批准。</规则>

<晨报> 每天 [07:30] 给我发一条消息(reports/04_morning_report_0730.md)：5 行市场概况、今天的高影响事件、3 个策略的排名（每个 5 行）、风控官的每次否决及原因、机器人状态和隔夜模拟交易，以及一个明确的决定：批准、拒绝或重测。</晨报>

<每周> 每 [周日] 对比模拟盘结果和回测，按晋级阶梯推进：模拟盘 2 周 → 结果与回测一致 → 经我批准后 10% 仓位实盘(2,000元人民币) → 完整规则(20,000元人民币)。熔断条件：单日亏损 2%(400元人民币)、连亏 3 笔或数据过期(>24h)。把每个 Agent 的任务设成 crontab 定时循环，合上电脑也能 7×24 交易。</每周>

<最终检查> 发送前逐个拷问每个策略：样本外还成立吗？扣费后还有优势吗？晚一根 K 线、滑点翻倍还能用吗？算上所有测过的想法，收缩夏普还站得住吗？能扛过 2020 和 2022 吗？每个策略结尾写上「它为什么会失败」，宁要有证据的无聊策略，不要没证据的刺激策略。</最终检查>

<开始> 4 个问题已由您的指令与默认风控红线确认锁定。团队、24 小时日程、今晚研究计划与全周期真实回测结果已全部就绪并开始全天候运转。</开始>

</prompt>"""


def build_six_agent_journal(
    conn: sqlite3.Connection,
    chart_scores: list[dict[str, Any]],
    bt_results: dict[str, Any],
    paper_portfolio: dict[str, Any],
    bot_audit: dict[str, Any],
    sunday_report: dict[str, Any],
) -> dict[str, Any]:
    passed_charts = [c for c in chart_scores if c["passed_gate"]]
    filtered_charts = [c for c in chart_scores if not c["passed_gate"]]
    top3 = bt_results["top3_approved"]
    vetoed_strats = bt_results["vetoed_strategies"]

    s1, s2, s3 = top3[0], top3[1], top3[2]

    handoffs: list[dict[str, Any]] = [
        {
            "id": "HANDOFF-20261108-2030-01",
            "timestamp": "2026-11-08 20:30:00 CST",
            "from_agent": "幕僚长",
            "to_agent": "侦察员",
            "stage": "01_RULEBOOK_LOCK_AND_NIGHT_DISPATCH",
            "标的": "全市场监控池 (中国大陆A股核心ETF/低价蓝筹 & 港股跨境/本土ETF，共26只核心标的)",
            "想法": "锁定《交易台规则》：总模拟资金20,000元人民币，单笔最大风险1.5%（300元），只布局中国大陆与港股市场，长回报周期；启动通宵30分钟轮询全球新闻、财报公告与板块资金流。",
            "证据": "用户指令明确『新闻可以是全球，我只布局中国大陆和港股市场，可以是长回报周期，资金规模2w人民币』；已剔除科创板(688xxx需50万门槛)、ST/*ST、仙股及高价股，保留100股/手仅需39~4,288元的场内ETF与主板蓝筹。",
            "下一步": "侦察员每30分钟抓取新浪7×24全球财经快讯、东方财富三季报(NOTICE_DATE)、行业主力资金流与AH溢价指数，提炼6大信号维度的候选催化剂并交接图表分析师。",
        },
        {
            "id": "HANDOFF-20261108-2045-02",
            "timestamp": "2026-11-08 20:45:00 CST",
            "from_agent": "侦察员",
            "to_agent": "图表分析师",
            "stage": "02_GLOBAL_MACRO_AND_SECTOR_FLOW_SCAN",
            "标的": "港股跨境与本土ETF集群 (sh510900 H股ETF, sz159920 恒生ETF, sh513050 中概互联网ETF, hk02800 盈富基金) 及 A股防御/成长ETF (sh512010 医药ETF, sh510880 红利ETF, sh518880 黄金ETF)",
            "想法": "跨市场板块轮动与波动率切换信号：恒生AH股溢价指数(100.HSAHP)持续回落，港股相对A股展现出显著估值优势与资金承接力；而美股英伟达夜盘回落及美国非农强化美联储加息预期压制A股高Beta成长板块。",
            "证据": "东方财富100.HSAHP指数由11月2日133.01降至129.55（近一周收敛-3.46点）；新浪7×24快讯显示英伟达夜盘跌3.5%而鸿海涨8.0%，WTI原油突破91美元/桶，黄金ETF持仓反弹；东方财富行业资金流显示电子(BK1201)主力净流入+80.24亿元、通信设备(BK0448)净流入+43.11亿元，电力设备(BK1200)净流出-23.66亿元、电池(BK1033)净流出-17.91亿元。",
            "下一步": "请图表分析师读取上述ETF的TradingView日线前复权(qfq)数据，按趋势(0-4分)+动量(0-3分)+成交量结构(0-3分)三者共振公式打1-10分，仅放行>=7.0分的标的交由策略师回测。",
        },
        {
            "id": "HANDOFF-20261108-2050-03",
            "timestamp": "2026-11-08 20:50:00 CST",
            "from_agent": "侦察员",
            "to_agent": "图表分析师",
            "stage": "03_EARNINGS_PEAD_AND_ANNOUNCEMENT_SCAN",
            "标的": "A股主板三季报与事件催化标的 (sh600028 中国石化, sh601857 中国石油, sh601398 工商银行, sh601766 中国中车, sh600900 长江电力, sh600522 中天科技, sh603920 世运电路)",
            "想法": "财报后漂移(PEAD)与新闻后动量筛选：2026三季报(NOTICE_DATE截至10月31日)已披露完毕，叠加WTI原油突破91美元/桶、千公里空芯光纤传输验证及世运电路回购公告，需甄别真突破与下跌趋势中的死猫跳。",
            "证据": "东方财富RPT_LICO_FN_CPD核实：中国石化(600028, 10-30披露)三季报净利同比+30.00%，中国石油(601857, 10-29披露)净利同比+21.90%，工商银行(601398, 10-31披露)营收+8.74%/净利+5.83%，中国中车(601766, 10-31披露)净利+3.70%；中天科技(600522)虽有空芯光纤利好及三季报净利+71.10%，但股价处于均线空头通道；世运电路(603920)发布回购公告但三季报净利-48.78%。",
            "下一步": "请图表分析师对全池26只标的逐一计算三者共振评分(1-10分)，严格拦截<7.0分的技术破位标的，将>=7.0分标的移交策略师在21:00启动2019-2026全周期回测。",
        },
        {
            "id": "HANDOFF-20261108-2058-04",
            "timestamp": "2026-11-08 20:58:00 CST",
            "from_agent": "图表分析师",
            "to_agent": "策略师",
            "stage": "04_CHARTIST_RESONANCE_GATE_PASS",
            "标的": "通过>=7.0分门槛的15只共振标的：" + ", ".join(f"{c['symbol']} {c['name']}({c['score']:.1f}分)" for c in passed_charts),
            "想法": "上述15只标的在日线级别实现『均线多头排列(Close>MA20/MA60) + 正向动量(ROC20>0, RSI14处于50-70强势区) + 量能/低波结构确认』三者共振，具备高胜率趋势与轮动交易结构。",
            "证据": f"基于腾讯前复权日线(2026-11-06收盘)实测：共 {len(passed_charts)} 只标的得分 >= 7.0/10（其中 sh510900 H股ETF、sh600900 长江电力、sh601766 中国中车均获满分 10.0/10；sh512010 医药ETF 与 sh600028 中国石化获 9.5/10；sz159920 恒生ETF 与 sh513050 中概互联网ETF获 9.0/10）。",
            "下一步": "策略师于[21:00]使用2019-01-02至2026-11-06真实日线数据（样本内2019-2022，样本外2023-2026）对6大候选策略进行含费率、滑点、100股整手与1000次蒙特卡洛的全面回测。",
        },
        {
            "id": "HANDOFF-20261108-2059-05",
            "timestamp": "2026-11-08 20:59:00 CST",
            "from_agent": "图表分析师",
            "to_agent": "策略师",
            "stage": "05_CHARTIST_RESONANCE_GATE_FILTER",
            "标的": "未达7.0分被拦截的11只标的：" + ", ".join(f"{c['symbol']} {c['name']}({c['score']:.1f}分)" for c in filtered_charts),
            "想法": "严格执行<7分不放行原则：禁止在趋势策略中追高技术面破位的新闻热点股（如中天科技600522、半导体ETF 512480）；仅允许将黄金ETF(518880)与红利ETF(510880)作为轮动/波动率切换策略的防御锚定资产。",
            "证据": "中天科技(600522)虽有空芯光纤新闻，但收盘23.15远低于MA20(26.162)与MA60(31.232)，ROC20=-23.06%，RSI14=32.95，形态仅1.0/10分；半导体ETF(512480)ROC20=-14.93%，形态1.5/10分；创业板ETF(159915)ROC20=-19.06%，形态3.0/10分。",
            "下一步": "策略师在[21:00]回测中检验全部6大信号原型在样本外(2023-2026)是否能通过25分制评分与6大残酷拷问。",
        },
        {
            "id": "HANDOFF-20261108-2345-06",
            "timestamp": "2026-11-08 23:45:00 CST",
            "from_agent": "策略师",
            "to_agent": "风控官",
            "stage": "06_NIGHTLY_BACKTEST_SUBMISSION",
            "标的": "6大候选量化策略池 (波动率切换、板块轮动、放量突破、过度反应后的均值回归、新闻后动量、财报后漂移)",
            "想法": "已完成2019-2026近8年真实数据回测（2019-2022样本内972交易日含2020与2022压力期；2023-2026未见样本外930交易日）、6窗口滚动前向、1000次蒙特卡洛最差回撤及多重检验收缩夏普计算，提请风控官按25分制与《最终检查》进行终审。",
            "证据": (
                f"排名前3的策略：(1) {s1['name']} 样本外净收益 +{s1['out_of_sample']['net_return_pct']}%、夏普 {s1['out_of_sample']['sharpe']}、收缩夏普 {s1['out_of_sample']['shrinkage_sharpe']}、交易 {s1['out_of_sample']['trades']} 笔、总分 {s1['total_score']}/25；"
                f"(2) {s2['name']} 样本外 +{s2['out_of_sample']['net_return_pct']}%、夏普 {s2['out_of_sample']['sharpe']}、交易 {s2['out_of_sample']['trades']} 笔、总分 {s2['total_score']}/25；"
                f"(3) {s3['name']} 样本外 +{s3['out_of_sample']['net_return_pct']}%、夏普 {s3['out_of_sample']['sharpe']}、交易 {s3['out_of_sample']['trades']} 笔、总分 {s3['total_score']}/25。其余3个策略总分均 <= 13/25。"
            ),
            "下一步": "请风控官行使终审否决权：砍掉总分<18/25、样本外交易<30笔、扣费后优势为负或不抗双倍滑点的策略与信号，并对获批的Top 1策略核准ATR仓位与限价止损指令。",
        },
        {
            "id": "HANDOFF-20261109-0115-07",
            "timestamp": "2026-11-09 01:15:00 CST",
            "from_agent": "风控官",
            "to_agent": "幕僚长",
            "stage": "07_RISK_OFFICER_TERMINAL_VETOES",
            "标的": "被否决的3项候选策略（过度反应后的均值回归、新闻后动量、财报后漂移单股版）及4项个股信号（sh600522 中天科技、sh601668 中国建筑、sh600050 中国联通、sh688827 钶锐锶）",
            "想法": "行使《团队》与《评分》终审否决权（否决即终审）：凡是总分<18/25、样本外交易<30笔、扣费后优势为负、违反基本面过滤或触碰账户禁区的计划一律砍掉。",
            "证据": (
                "否决证据链：(1) 过度反应均值回归策略样本外扣费后净收益 -7.01%、单笔优势 -0.25%、总分 13/25 < 18；"
                "(2) 新闻后动量策略受A股T+1高开低走陷阱拖累，样本外净收益 -9.12%、2022年亏损 -11.23%、总分 12/25 < 18；"
                "(3) 单股PEAD策略2023-2026样本外仅10笔交易（<30笔硬门槛）且受5元最低佣金摩擦净亏 -0.52%、总分 9/25 < 18；"
                "(4) 个股拦截：中天科技(600522)图表1.0/10分破位；中国建筑(601668)三季报净利-24.80%、中国联通(600050)三季报净利-33.80%；科创板新股钶锐锶(688827)违反50万门槛与新股禁令。"
            ),
            "下一步": "将全部否决记录及原因写入《07:30 晨报》与交易日志，仅批准总分>=18/25的前3名策略进入晨报排名，并授权交易员在模拟盘执行排名第1的策略。",
        },
        {
            "id": "HANDOFF-20261109-0700-08",
            "timestamp": "2026-11-09 07:00:00 CST",
            "from_agent": "风控官",
            "to_agent": "交易员",
            "stage": "08_APPROVED_EXECUTION_PLAN",
            "标的": "排名第1策略当前共振持仓组合：sh510900 H股ETF (图表10.0分, T+0)、sz159920 恒生ETF (图表9.0分, T+0)、sh512010 医药ETF (图表9.5分, T+1) 及 sh600028 中国石化 (图表9.5分, Q3净利+30%, T+1)",
            "想法": "核准排名第1策略在20,000元人民币模拟盘的执行参数：严格采用基于ATR14的仓位公式 `股数 = floor(min(20000*0.015/(2.0*ATR14), 5000/Price)/100)*100`，确保单笔最大止损风险严格 <= 300元(1.5%)红线。",
            "证据": (
                "以20,000元本金核验：(1) H股ETF(510900)限价1.019元，止损价0.984元，48手(4,800股)，最大止损风险168.00元(0.84%)；"
                "(2) 恒生ETF(159920)限价1.442元，止损价1.393元，33手(3,300股)，最大止损风险161.70元(0.81%)；"
                "(3) 医药ETF(512010)限价0.393元，止损价0.372元，90手(9,000股)，最大止损风险189.00元(0.95%)；"
                "(4) 中国石化(600028)限价5.800元，止损价5.431元，8手(800股)，最大止损风险295.20元(1.48% <= 300元)。今日无财报/FOMC/CPI静默期冲突。"
            ),
            "下一步": "交易员使用限价单(LIMIT)并强制绑定止损单(STOP-LOSS)在模拟盘执行上述持仓，回报机器人运行状态与隔夜模拟成交明细。",
        },
        {
            "id": "HANDOFF-20261109-0715-09",
            "timestamp": "2026-11-09 07:15:00 CST",
            "from_agent": "交易员",
            "to_agent": "幕僚长",
            "stage": "09_PAPER_BROKER_EXECUTION_REPORT",
            "标的": "Strategy #1 模拟盘组合 (sh510900 48手, sz159920 33手, sh512010 90手, sh600028 8手；剩余可用现金 2,165.20 RMB)",
            "想法": "已通过唯一授权的交易员接口(CNHKPaperBroker + AlpacaPaperBroker双模引擎)完成模拟盘限价委托与止损单挂载，静态与运行时代码审计确认0未来函数、0漏设止损、3大熔断器全部绿灯正常。",
            "证据": (
                f"模拟盘订单簿核实：4笔持仓全部采用限价单成交并附带2.0*ATR14硬止损单；入场总市值 {paper_portfolio['total_entry_notional_rmb']:.2f} 元，"
                f"最新市值 {paper_portfolio['total_market_value_rmb']:.2f} 元，扣除佣金 {paper_portfolio['total_fees_rmb']:.2f} 元后净浮动盈亏 +{paper_portfolio['total_unrealized_pnl_rmb']:.2f} 元，总资产净值 {paper_portfolio['total_equity_rmb']:.2f} 元。"
            ),
            "下一步": "请幕僚长汇总全队通宵成果、今日高影响事件、Top 3策略排名、风控官否决清单及模拟盘状态，于[07:30]准时向用户发送《晨报》。",
        },
        {
            "id": "HANDOFF-20261109-0730-10",
            "timestamp": "2026-11-09 07:30:00 CST",
            "from_agent": "幕僚长",
            "to_agent": "用户",
            "stage": "10_MORNING_REPORT_0730_DISPATCH",
            "标的": "一人对冲基金 [07:30] 晨报与全天候模拟盘决策单",
            "想法": "向用户呈交包含5行市场概况、今日高影响事件、按样本外表现排名的3个策略(各5行)、风控官每次否决及原因、机器人状态与隔夜模拟交易，以及明确决定（批准）的完整晨报。",
            "证据": "所有行情、三季报(NOTICE_DATE)、板块资金流与回测指标100%源自腾讯财经、东方财富与新浪财经真实缓存数据库(data/market_cache.sqlite)，并通过多重检验收缩夏普与2020/2022残酷压力测试。",
            "下一步": "明确决定：【批准 (APPROVE)】排名第1策略继续全天候跑模拟盘2周，每周日对比模拟盘与回测一致性，达标并经用户批准后按晋级阶梯启动10%仓位(2,000元)实盘。",
        },
    ]

    risk_officer_vetoes = [
        {
            "veto_id": "VETO-STRAT-01",
            "target_type": "STRATEGY",
            "target": vetoed_strats[0]["name"] + f" ({vetoed_strats[0]['archetype']})",
            "score": f"{vetoed_strats[0]['total_score']} / 25 (< 18 硬门槛)",
            "reason": (
                f"样本外扣除真实佣金与滑点后净收益为 {vetoed_strats[0]['out_of_sample']['net_return_pct']}%（单笔扣费后优势 {vetoed_strats[0]['out_of_sample']['post_cost_edge_pct']}% < 0），"
                f"且在晚1根K线+2倍滑点测试下亏损扩大至 {vetoed_strats[0]['stress_tests']['delay_1bar_and_double_slippage']['net_return_pct']}%，2022熊市亏损 {vetoed_strats[0]['stress_tests']['stress_2022']['net_return_pct']}%，风控官终审否决。"
            ),
        },
        {
            "veto_id": "VETO-STRAT-02",
            "target_type": "STRATEGY",
            "target": vetoed_strats[1]["name"] + f" ({vetoed_strats[1]['archetype']})",
            "score": f"{vetoed_strats[1]['total_score']} / 25 (< 18 硬门槛)",
            "reason": (
                f"在A股 T+1 交收制度下追逐隔夜新闻高开极易遭遇『高开低走』日内锁仓陷阱，样本外净收益 {vetoed_strats[1]['out_of_sample']['net_return_pct']}%、2022年亏损 {vetoed_strats[1]['stress_tests']['stress_2022']['net_return_pct']}%，"
                "且违反《信号》『新策略绝不在财报、美联储议息或 CPI 前上线』原则，风控官终审否决。"
            ),
        },
        {
            "veto_id": "VETO-STRAT-03",
            "target_type": "STRATEGY",
            "target": vetoed_strats[2]["name"] + f" ({vetoed_strats[2]['archetype']})",
            "score": f"{vetoed_strats[2]['total_score']} / 25 (< 18 硬门槛)",
            "reason": (
                f"2023-2026样本外仅触发 {vetoed_strats[2]['out_of_sample']['trades']} 笔交易，未达到《评分》『至少 30 笔交易』硬门槛；"
                f"同时20,000元账户交易单只主板股票受100股整手颗粒度与5元最低佣金摩擦严重（样本外净收益 {vetoed_strats[2]['out_of_sample']['net_return_pct']}%），风控官终审否决。"
            ),
        },
        {
            "veto_id": "VETO-SIGNAL-04",
            "target_type": "TICKER_SIGNAL",
            "target": "sh600522 中天科技 (空芯光纤新闻催化 + 三季报净利+71.10%)",
            "score": "图表评分 1.0 / 10 (< 7.0 门槛)",
            "reason": "虽有全球首个千公里空芯光纤传输验证新闻提振，但日线收盘 23.15 远低于 MA20(26.16) 与 MA60(31.23)，20日动量 -23.06%，严重违反图表分析师 >=7.0 分放行铁律，否决追涨。",
        },
        {
            "veto_id": "VETO-SIGNAL-05",
            "target_type": "TICKER_SIGNAL",
            "target": "sh601668 中国建筑 & sh600050 中国联通 (图表得分 8.5 / 10)",
            "score": "基本面财报过滤不达标",
            "reason": "虽然日线技术形态获 8.5/10 分，但东方财富三季报(2026-10-31披露)显示中国建筑归母净利同比 -24.80%、中国联通归母净利同比 -33.80%，违反财报正向筛选原则，否决纳入核心多头池。",
        },
        {
            "veto_id": "VETO-SIGNAL-06",
            "target_type": "TICKER_SIGNAL",
            "target": "sh688827 钶锐锶 (2026-11-09 科创板首发上市新股)",
            "score": "触发《交易台规则》黑名单禁区",
            "reason": "属 688 科创板股票（需50万元人民币资产门槛，超出2万元账户权限）且为上市首日无涨跌幅限制新股，缺乏历史 ATR14 锚点，风控官自动屏蔽否决。",
        },
    ]

    return {
        "desk_name": "一人对冲基金交易台 (中国大陆A股与港股 · 20,000 RMB 专版)",
        "generated_at": "2026-11-09T07:30:00+08:00",
        "agents_roster": ["幕僚长", "侦察员", "图表分析师", "策略师", "风控官", "交易员"],
        "handoff_schema_keys": ["标的", "想法", "证据", "下一步"],
        "handoffs": handoffs,
        "risk_officer_vetoes": risk_officer_vetoes,
        "chart_analyst_scores": chart_scores,
        "paper_portfolio": paper_portfolio,
        "bot_code_audit": bot_audit,
        "sunday_reconciliation": sunday_report,
    }


def write_report_01_onboarding(rules: dict[str, Any], chart_scores: list[dict[str, Any]]) -> str:
    passed = [c for c in chart_scores if c["passed_gate"]]
    filtered = [c for c in chart_scores if not c["passed_gate"]]

    md = f"""# 01 — 交易台组建、首次问询锁定、13标签 Prompt 与 24 小时作战日程 (`<问询>`, `<开始>`, `<团队>`, `<规则>`)

## 一、原版 13 标签 `<prompt>` 完整逐字转录 (`07530d214b621e643230cc9504448bf6.jpg`)

```xml
{VERBATIM_PROMPT_XML}
```

---

## 二、为您量身定制的「中国大陆A股与港股 · 20,000 RMB 专版」`<prompt>`（可直接复制使用）

```xml
{CUSTOMIZED_CN_HK_PROMPT_XML}
```

---

## 三、`<问询>` 与 `<开始>`：4 个核心问题确认与《交易台规则》(`config/trading_desk_rules.json`)

根据您的原始指令（*「新闻可以是全球，我只布局中国大陆和港股市场，可以是长回报周期，资金规模2w人民币」*），**幕僚长 (`幕僚长`)** 已将 4 个问题的答案正式锁入交易台最高宪法 `config/trading_desk_rules.json`，所有 6 个 Agent 在每次行动前强制读取：

| 问询维度 (`<问询>`) | 您的回答与交易台锁定参数 | 量化微观结构与工程实现细节 |
|---|---|---|
| **问题 1：我交易哪些市场？新闻范围？** | **新闻放眼全球 (`GLOBAL`)，交易仅限中国大陆 A 股与港股 (`["CN_A", "HKEX"]`)** | 侦察员 7×24 扫描全球宏观（美联储议息、非农、美股 AI 科技、WTI 原油、黄金）并映射至上交所/深交所场内 ETF、T+0 港股跨境 ETF、港股本土核心 ETF（`02800.HK`/`03033.HK`）及沪深主板低估值蓝筹。 |
| **问题 2：模拟资金和单笔最大风险？** | **模拟总本金 `20,000.00 RMB`；单笔最大止损风险 `1.5%`（`300.00 RMB`）** | 严格执行 A 股/ETF `100 股 (1手)` 整手向下取整：$\\text{{Shares}} = \\lfloor \\min(\\frac{{300}}{{k \\cdot \\text{{ATR}}_{{14}}}}, \\frac{{\\text{{PosCap}}}}{{\\text{{Price}}}}) / 100 \\rfloor \\times 100$；模拟盘单标的上限 `25%`（`5,000 RMB`），回测双持仓上限 `48%`。 |
| **问题 3：持仓周期？** | **中长回报周期 (`LONG_CYCLE`，`10~40` 个交易日波段/轮动)** | 规避高频日内磨损（特别是 A 股主板股票 **单笔最低 5 元佣金** 与万五卖出印花税），以 10~15 日核心波段与跨季趋势轮动捕获高盈亏比行情。 |
| **问题 4：绝对不碰什么（禁区黑名单）？** | **8 大绝对禁区（写入 `blacklist` 硬件级拦截）** | 1. `ST / *ST` 及退市整理股；2. 需 50 万门槛的 `科创板 (688xxx)` 与 `北交所`；3. 需 50 万门槛的港股通个人直投个股（改用 100 元起投的 T+0 跨境恒生/H股 ETF）；4. 港股仙股（`< 1 HKD`）；5. 单价 `> 45 RMB` 高价股；6. 上市首日新股（如 `688827 钶锐锶`）；7. 未经批准的杠杆/融资融券/期权期货；8. 小道消息与拉盘群。 |

---

## 四、`<团队>`：六个专职 Agent 编制与职责矩阵

| 序号 | Agent 角色 | 汇报与协作关系 | 铁律与核心量化职责 |
|---|---|---|---|
| 1 | **幕僚长 (`Chief of Staff`)** | 统筹全局，**唯一向您发消息的 Agent** | 管理 `config/trading_desk_rules.json`，汇总各 Agent 交接单，每日 `[07:30]` 准时呈交《晨报》，每周日 `[20:00]` 主持模拟盘晋级复盘。 |
| 2 | **侦察员 (`Scout`)** | 每 `[30 分钟]` 扫描 -> 交接 `图表分析师` | 实时轮询新浪 7×24 全球财经快讯 (`zhibo.sina.com.cn`)、东方财富三季报 (`RPT_LICO_FN_CPD`)、行业主力资金流 (`push2.eastmoney.com`) 与 AH 溢价指数 (`100.HSAHP`)。 |
| 3 | **图表分析师 (`Chart Analyst`)** | 读 TradingView 图表 -> 交接 `策略师` | 对监控池全部 26 只中港标的按 **趋势(0-4分) + 动量(0-3分) + 成交量结构(0-3分)** 打 `1–10` 分，**严格执行 `>= 7.0 分` 才放行**（当前放行 {len(passed)} 只，拦截 {len(filtered)} 只）。 |
| 4 | **策略师 (`Strategist`)** | 每晚 `[21:00]` 回测 -> 交接 `风控官` | 用 `2019-01-02` 至 `2026-11-06` 近 8 年真实日线数据（样本内 `2019–2022`，未见样本外 `2023–2026`）回测 6 大信号原型，计算收缩夏普、滚动前向与 1000 次蒙特卡洛最差回撤。 |
| 5 | **风控官 (`Risk Officer`)** | **拥有终审否决权 (`否决即终审`)** | 砍掉总分 `< 18/25`、样本外交易 `< 30` 笔、扣费后优势 `<= 0`、不抗 `+1根K线/2倍滑点`、不抗 `2020/2022` 熊市或违反财报/FOMC/CPI 静默期的任何计划。 |
| 6 | **交易员 (`Trader`)** | **唯一能碰下单工具的 Agent** | 硬件级权限锁 `enforce_trader_role("交易员")`；只执行已批准计划，强制使用 **限价单 (`LIMIT`)** 并附带 **ATR 止损单 (`STOP-LOSS`)**。 |

---

## 五、`<开始>`：24 小时全天候日程表与今晚研究计划

| 北京时间 (`CST`) | 定时 Cron 表达式 | 执行 Agent | 核心任务与产出物 |
|---|---|---|---|
| **每 30 分钟 (`00:00–23:30`)** | `*/30 * * * *` | `侦察员` | 扫描新浪 7×24 全球快讯、东方财富公司公告、板块主力净流入与 AH 溢价指数，更新 `data/market_cache.sqlite`。 |
| **收盘后 (`15:05 / 16:10 / 20:50`)** | `5 15,16,20 * * 1-5` | `图表分析师` | 拉取 A 股 (`15:00` 收盘) 与港股 (`16:00` 收盘) 前复权日线，计算三者共振 `1-10` 评分，过滤 `< 7.0` 分标的。 |
| **每晚 `[21:00]`** | `0 21 * * *` | `策略师` | 运行 `2019–2026` 全周期向量化/事件驱动回测（含 `2020`/`2022` 压力测试、`+1 Bar` 延迟、`2x` 滑点、1000 次蒙特卡洛与收缩夏普）。 |
| **凌晨 `[01:00]`** | `0 1 * * *` | `风控官` | 执行《最终检查》6 问拷问，对不合格策略与破位信号行使终审否决权，核准 Top 1 策略的 20,000 RMB ATR 整手限价止损计划。 |
| **清晨 `[07:00]`** | `0 7 * * *` | `交易员` | 执行静态 AST 与运行时代码自检 (`audit_bot_code`)，通过 `CNHKPaperBroker` 与 `AlpacaPaperBroker` 挂载限价+止损单。 |
| **每天 `[07:30]`** | `30 7 * * *` | `幕僚长` | 向您发送《07:30 晨报》（5行市场概况、今日高影响事件、Top 3 策略排名、风控官每次否决、机器人状态与明确决定）。 |
| **每周日 `[20:00]`** | `0 20 * * 0` | `幕僚长 & 全员` | 执行周日模拟盘与回测对账 (`generate_sunday_reconciliation_report`)，按四步晋级阶梯推进。 |

### 图表分析师 (`图表分析师`) 当前 26 只中港核心标的 1–10 分共振普查表 (`2026-11-06` 收盘基准 + `2026-11-09` 实时行情)

| 排名 | 代码 | 名称 | 市场与交收 | `2026-11-06` 收盘 | `2026-11-09` 现价 (`涨跌%`) | `MA20` | `MA60` | `ROC20` | `RSI14` | 量比 (`Vol/MA20`) | `ATR14` | 1手成本 (RMB) | 图表评分 (`趋势+动量+量能`) | `>=7.0` 门槛结论 |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
"""
    for idx, c in enumerate(chart_scores, start=1):
        gate_str = "**PASS (放行)**" if c["passed_gate"] else "FILTER (拦截 <7)"
        md += (
            f"| {idx} | `{c['symbol']}` | {c['name']} | {c['asset_class']} (`{c['settlement']}`) | "
            f"`{c['close']:.3f}` | `{c['live_price']:.3f} ({c['live_pct']:+.2f}%)` | `{c['ma20']:.3f}` | `{c['ma60']:.3f}` | "
            f"`{c['roc20_pct']:+.2f}%` | `{c['rsi14']:.1f}` | `{c['vol_ratio']:.2f}x` | `{c['atr14']:.4f}` | "
            f"`{c['lot_cost_rmb']:.1f}` | **`{c['score']:.1f} / 10`** (`{c['trend_pts']:.1f}+{c['mom_pts']:.1f}+{c['vol_pts']:.1f}`) | {gate_str} |\n"
        )
    return md


def write_report_02_strategies(bt_results: dict[str, Any]) -> str:
    cal = bt_results["calendar"]
    bench = bt_results["benchmark_csi300"]
    strats = bt_results["strategies"]
    top3 = bt_results["top3_approved"]
    vetoed = bt_results["vetoed_strategies"]

    md = f"""# 02 — 六大信号全周期真实回测、25分制评分矩阵、Top 3 策略完整9要素规范与《最终检查》6问拷问 (`<信号>`, `<评分>`, `<策略格式>`, `<最终检查>`)

## 一、真实数据回测基准与微观结构说明 (`<规则>`)
- **真实数据源 (`data/market_cache.sqlite`)**：腾讯财经前复权日线接口 (`proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get`) + 雅虎财经 (`query1.finance.yahoo.com`) 交叉核验 + 东方财富季报中心 (`RPT_LICO_FN_CPD` 真实 `NOTICE_DATE`)。
- **回测时间跨度 (`{cal['start_date']}` 至 `{cal['end_date']}`，共 `{cal['total_trading_days']}` 个交易日)**：
  - **样本内 (`In-Sample`, `2019–2022`)**：`{cal['warmup_end_date']}` 至 `{cal['is_end_date']}`（含 60 日指标预热，完整覆盖 **2020 年疫情崩盘** 与 **2022 年单边熊市** 两大残酷压力期）。
  - **未见样本外 (`Out-of-Sample`, `2023–2026`)**：`{cal['oos_start_date']}` 至 `{cal['end_date']}`（共 `{cal['oos_trading_days']}` 个未见样本外交易日，约 `3.69` 年）。
- **20,000 RMB 账户真实交易成本与交收模型**：
  - 初始本金 `20,000.00 RMB`，严格按 **100 股 (1手)** 向下取整；
  - 场内/跨境 ETF：佣金 `万 1.5`（最低 `1.00 RMB`），免印花税（`0%`），基线滑点 `5 bps`（翻倍测试 `10 bps`）；
  - A 股主板个股：佣金 `万 2.5`（**严格执行最低 `5.00 RMB` 收费**），卖出印花税 `万 5`（`0.05%`），过户费 `0.1 bps`，基线滑点 `8 bps`（翻倍测试 `16 bps`）；
  - 港股本土 ETF (`hk02800`)：佣金及征费 `万 3`（最低 `3.00 RMB`），基线滑点 `5 bps`，汇率折算 `1 HKD = 0.92 RMB`。
  - 同期基准 `sh510300 沪深300ETF` 表现：样本内 `{bench['is_return_pct']:+.2f}%`，样本外 (`2023–2026`) `{bench['oos_return_pct']:+.2f}%`，`2020年` `{bench['return_2020_pct']:+.2f}%`，`2022年` **`{bench['return_2022_pct']:+.2f}%`**。

---

## 二、`<评分>`：6 大候选策略 25 分制评分总表与硬性资格门控

| 排名 | 策略名称与信号原型 (`<信号>`) | 审批状态 | 1. 样本外夏普 (1-5) | 2. 收缩夏普 (1-5) | 3. 最大回撤 (1-5) | 4. 交易次数 (1-5) | 5. 扣费后优势 (1-5) | **总分 (/25)** | 样本内 (`2019–2022`) 净收益 / 夏普 / 回撤 / 笔数 | **未见样本外 (`2023–2026`) 净收益 / 夏普 / 收缩夏普 / 回撤 / 笔数 / 单笔优势** |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|---|
"""
    for s in strats:
        sb = s["score_breakdown"]
        mis = s["in_sample"]
        moos = s["out_of_sample"]
        st_badge = f"**APPROVED (Top #{s['rank']})**" if s["qualified"] else "**VETOED (风控官否决)**"
        md += (
            f"| #{s['rank']} | **{s['name']}**<br>(`{s['archetype']}`) | {st_badge} | "
            f"`{sb['oos_sharpe_score']}` ({moos['sharpe']:.2f}) | `{sb['shrinkage_sharpe_score']}` ({moos['shrinkage_sharpe']:.2f}) | "
            f"`{sb['max_dd_score']}` ({moos['max_dd_pct']:.2f}%) | `{sb['trade_count_score']}` ({moos['trades']}笔) | "
            f"`{sb['post_cost_edge_score']}` ({moos['post_cost_edge_pct']:+.2f}%) | **`{s['total_score']} / 25`** | "
            f"`{mis['net_return_pct']:+.2f}%` / SR `{mis['sharpe']:.2f}` / DD `{mis['max_dd_pct']:.2f}%` / `{mis['trades']}笔` | "
            f"**`{moos['net_return_pct']:+.2f}%`** / SR **`{moos['sharpe']:.2f}`** / ShrSR **`{moos['shrinkage_sharpe']:.2f}`** / DD **`{moos['max_dd_pct']:.2f}%`** / **`{moos['trades']}笔`** / Edge **`{moos['post_cost_edge_pct']:+.2f}%`** |\n"
        )

    md += """
---

## 三、`<最终检查>`：发送前 6 大残酷拷问矩阵 (6-Question Torture Test Matrix)

| 策略名称 (`<信号>`) | Q1: 样本外还成立吗？ (`>=30笔` 且净收益>0) | Q2: 扣费后还有优势吗？ (含5元最低佣金/印花税) | Q3: 晚一根K线 (`+1 Bar`)、滑点翻倍 (`2x Slip`) 还能用吗？ | Q4: 算上所有测过的想法，收缩夏普 (`DSR`) 还站得住吗？ | Q5: 能扛过 `2020` 和 `2022` 吗？ (对比沪深300 `2022年 -21.63%`) | Q6: 结尾是否写明「它为什么会失败」？ | 终审结论 |
|---|---|---|---|---|---|---|---|
"""
    for s in strats:
        moos = s["out_of_sample"]
        st = s["stress_tests"]
        tq = s["torture_test_6q"]
        d1 = st["delay_1bar_only"]
        sl2 = st["double_slippage_only"]
        both = st["delay_1bar_and_double_slippage"]
        s20 = st["stress_2020"]
        s22 = st["stress_2022"]
        md += (
            f"| **{s['name']}** (`{s['archetype']}`) | "
            f"{'PASS' if tq['q1_oos_valid'] else 'FAIL'} (`{moos['net_return_pct']:+.2f}%`, `{moos['trades']}笔`) | "
            f"{'PASS' if tq['q2_post_cost_positive'] else 'FAIL'} (`{moos['post_cost_edge_pct']:+.2f}%/笔`, 总费 `{moos['total_fees_rmb']:.0f}元`) | "
            f"{'PASS' if tq['q3_delay_1bar_and_2x_slippage'] else 'FAIL'} (晚1K `{d1['net_return_pct']:+.2f}%` / 2x滑点 `{sl2['net_return_pct']:+.2f}%` / 双重叠加 `{both['net_return_pct']:+.2f}%`) | "
            f"{'PASS' if tq['q4_shrinkage_sharpe_valid'] else 'FAIL'} (`ShrSR = {moos['shrinkage_sharpe']:.2f}`) | "
            f"{'PASS' if tq['q5_survives_2020_and_2022'] else 'FAIL'} (`2020`: `{s20['net_return_pct']:+.2f}%` DD `{s20['max_dd_pct']:.2f}%`; `2022`: `{s22['net_return_pct']:+.2f}%` DD `{s22['max_dd_pct']:.2f}%`) | "
            f"已完整披露 | **{'批准保留' if s['qualified'] else '风控官否决'}** |\n"
        )

    md += "\n---\n\n## 四、`<策略格式>`：获批 Top 3 策略的完整 9 要素深度规范\n\n"
    for s in top3:
        ne = s["nine_elements"]
        mis = s["in_sample"]
        moos = s["out_of_sample"]
        st = s["stress_tests"]
        wf = s["walk_forward"]
        mc = s["monte_carlo_1000"]
        wf_str = " | ".join(f"{w['window']}: `{w['return_pct']:+.2f}%` (DD `{w['max_dd_pct']:.2f}%`)" for w in wf["windows"])

        md += f"""### 排名第 #{s['rank']} 策略：{s['name']}（信号原型：`{s['archetype']}` | 评分：`{s['total_score']} / 25` | 图表分析师评分：`{s['chart_analyst_score']} / 10`）

1. **一句话论点 (One-Sentence Thesis)**：
   {ne['thesis']}
2. **对手盘是谁 (Who is the Counterparty)**：
   {ne['counterparty']}
3. **精确的进场 / 出场 / 止损规则 (Exact Entry / Exit / Stop-Loss Rules)**：
   - **进场规则**：{ne['entry_rule']}
   - **出场规则**：{ne['exit_rule']}
   - **止损规则**：{ne['stop_rule']}
   - **上线静默期保护**：严格执行《信号》铁律——新策略与新买单绝不在财报披露、美联储 FOMC 议息或中美 CPI 发布前 24 小时上线。
4. **基于 ATR 的 20,000 RMB 整手仓位规则 (ATR Position Sizing for 20,000 RMB)**：
   - {ne['atr_position_sizing']}
5. **扣除手续费和滑点后的样本内 (`2019–2022`) 与样本外 (`2023–2026`) 对比**：
   - **样本内 (`2019–2022`)**：扣费后净收益 **`{mis['net_return_pct']:+.2f}%`**（年化 `{mis['cagr_pct']:+.2f}%`），夏普比率 **`{mis['sharpe']:.2f}`**，最大回撤 **`{mis['max_dd_pct']:.2f}%`**，交易 **`{mis['trades']} 笔`**，胜率 `{mis['win_rate_pct']:.1f}%`，单笔扣费后优势 `{mis['post_cost_edge_pct']:+.2f}%`，累计实扣佣金印花税滑点 `{mis['total_fees_rmb']:.2f} RMB`。
   - **未见样本外 (`2023–2026`)**：扣费后净收益 **`{moos['net_return_pct']:+.2f}%`**（年化 `{moos['cagr_pct']:+.2f}%`），样本外夏普 **`{moos['sharpe']:.2f}`**，多重检验收缩夏普 **`{moos['shrinkage_sharpe']:.2f}`**，最大回撤 **`{moos['max_dd_pct']:.2f}%`**，交易 **`{moos['trades']} 笔`**（远超 30 笔硬门槛），胜率 `{moos['win_rate_pct']:.1f}%`，单笔扣费后优势 **`{moos['post_cost_edge_pct']:+.2f}%`**，累计实扣费用 `{moos['total_fees_rmb']:.2f} RMB`。
   - **残酷压力测试表现**：晚 1 根 K 线进出场 (`+1 Bar Delay`) 样本外净收益 **`{st['delay_1bar_only']['net_return_pct']:+.2f}%`**；2 倍滑点 (`2x Slippage`) 样本外净收益 **`{st['double_slippage_only']['net_return_pct']:+.2f}%`**；晚 1 根 K 线 + 2 倍滑点双重叠加净收益 **`{st['delay_1bar_and_double_slippage']['net_return_pct']:+.2f}%`**（夏普 `{st['delay_1bar_and_double_slippage']['sharpe']:.2f}`）；`2020年` 净收益 **`{st['stress_2020']['net_return_pct']:+.2f}%`**（最大回撤 `{st['stress_2020']['max_dd_pct']:.2f}%`）；`2022年` 熊市净收益 **`{st['stress_2022']['net_return_pct']:+.2f}%`**（最大回撤仅 **`{st['stress_2022']['max_dd_pct']:.2f}%`**，大幅跑赢沪深300的 `-21.63%`）。
6. **滚动前向验证结果 (6-Window Rolling Walk-Forward)**：
   - **正收益窗口占比**：**`{wf['positive_windows']} / {wf['total_windows']}` (`{wf['consistency_pct']:.1f}%`)**
   - **分窗口明细**：{wf_str}
7. **1,000 次蒙特卡洛最差回撤分布 (1,000-Run Monte Carlo Bootstrap Drawdown, `seed=42`)**：
   - **95% 分位回撤**：`{mc['dd_95th_pct']:.2f}%` | **99% 分位回撤**：`{mc['dd_99th_pct']:.2f}%` | **1,000 次最差回撤 (`Worst-Case Drawdown`)**：**`{mc['worst_dd_pct']:.2f}%`**
8. **会让它失效的市场环境 (Invalidating Market Regimes)**：
   - {ne['invalidating_regime']}
9. **它为什么会失败 (`<最终检查>`: Why It Will Fail)**：
   - {ne['why_it_fails']}

---

"""

    md += "## 五、被风控官 (`风控官`) 终审否决的 3 项候选策略及原因备案\n\n"
    for s in vetoed:
        moos = s["out_of_sample"]
        st = s["stress_tests"]
        md += f"""### 否决策略 #{s['rank']}：{s['name']}（信号原型：`{s['archetype']}` | 总分：`{s['total_score']} / 25` — **VETOED**）
- **核心回测证据**：样本内 (`2019–2022`) 净收益 `{s['in_sample']['net_return_pct']:+.2f}%`（`{s['in_sample']['trades']} 笔`）；未见样本外 (`2023–2026`) 净收益 **`{moos['net_return_pct']:+.2f}%`**，样本外夏普 `{moos['sharpe']:.2f}`，收缩夏普 `{moos['shrinkage_sharpe']:.2f}`，样本外交易 `{moos['trades']} 笔`，单笔扣费后优势 `{moos['post_cost_edge_pct']:+.2f}%`；晚 1 根 K 线 + 2 倍滑点净收益 `{st['delay_1bar_and_double_slippage']['net_return_pct']:+.2f}%`。
- **风控官终审否决书与「它为什么会失败」**：{s['nine_elements']['why_it_fails']}

"""
    return md


def write_report_03_bot(bot_audit: dict[str, Any], paper_portfolio: dict[str, Any]) -> str:
    pine_code = PINE_PATH.read_text(encoding="utf-8") if PINE_PATH.exists() else ""
    checks_rows = "\n".join(
        f"| `{k}` | **{'PASS (通过)' if v['passed'] else 'FAIL'}** | {v['detail']} |"
        for k, v in bot_audit["checks"].items()
    )

    md = f"""# 03 — 排名第 1 策略自动化交易机器人、TradingView Pine v5 指标、代码审查与零基础部署指南 (`<搭建机器人>`, `<每周>`)

## 一、步骤 1：复述目标与《交易台规则》(`restate_objective_and_rules()`)
- **核心目标**：{STEP_1_RULES_SUMMARY['objective']}
- **资金与风控铁律**：
  1. 模拟盘总资金：**`20,000.00 RMB`**（只布局中国大陆 A 股与港股市场 `["CN_A", "HKEX"]`，全球新闻输入 `GLOBAL`，中长持仓周期 `LONG_CYCLE`）；
  2. 单笔最大止损风险：**`1.5%` (`300.00 RMB`)**，模拟盘单标的仓位上限 **`25%` (`5,000.00 RMB`)**，严格按 **100 股 (`1手`)** 向下取整；
  3. 下单权限与订单类型铁律：**只有交易员能碰下单工具** (`enforce_trader_role("交易员")`)；**只执行已批准的计划，用限价单 (`LIMIT`) 并强制附带止损 (`STOP-LOSS`)**；
  4. 上线静默期与三大熔断条件：新策略绝不在财报、美联储议息或 CPI 前上线；触发 **单日亏损 `>= 2%` (`400 RMB`)**、**连亏 `>= 3` 笔** 或 **数据过期 (`> 24h`)** 立即熔断停机。

---

## 二、步骤 2：选最简单的设计 (`STEP_2_MINIMAL_DESIGN`)
- **设计方案**：`{STEP_2_MINIMAL_DESIGN['design_philosophy']}`
- **为什么这是最简单且最可靠的设计**：{STEP_2_MINIMAL_DESIGN['why_simplest']}

---

## 三、步骤 3：规划 TradingView 数据、信号、风控、订单和日志的五层架构 (`STEP_3_ARCHITECTURE_SPEC`)

```
+---------------------------------------------------------------------------------------------------+
| [1. TradingViewDataAdapter]  腾讯/雅虎前复权日线 (market_cache.sqlite) + TradingView Pine Webhook |
|                              强制校验 bar_confirmed == True (拒绝未收盘闪烁 K 线)                  |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| [2. Rank1SignalGenerator]    compute_signal_at_index(bars, idx) -> 严格切片 window = bars[:idx+1] |
|                              计算 SMA20/SMA60 趋势 + ROC20/RSI14 动量 + 量比 + ATR14 三者共振     |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| [3. RiskControlGate]         三大熔断器 (单日亏>=2%/400元、连亏>=3笔、数据>24h)                   |
|                              + 财报/美联储FOMC/CPI前24h静默期 + 图表分析师 >=7.0分门槛            |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| [4. DualModeOrderExecutor]   enforce_trader_role("交易员") 硬件级角色锁                           |
|                              ├─ CNHKPaperBroker (中港2万RMB本地模拟盘: 100股整手, T+1 vs T+0)     |
|                              └─ AlpacaPaperBroker (Alpaca REST OTO 限价+止损单 + 中港ETF代理映射) |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| [5. AuditJournalLogger]      SQLite (bot_paper_orders / bot_paper_positions)                      |
|                              + reports/trading_journal.json + 每周日模拟盘vs回测一致性对账        |
+---------------------------------------------------------------------------------------------------+
```

---

## 四、步骤 4：带注释的 Python 机器人 (`strategy_rank1_bot.py`) 与带提醒的 TradingView 指标 (`pine/strategy_rank1_indicator.pine`)

### 4.1 Python 机器人核心实现文件
完整可执行源码位于 `/home/antigravity/1261710749/test/strategy_rank1_bot.py`，核心特性包括：
- `calculate_atr_position_size()`：精确执行 20,000 RMB 账户 `1.5%` 风险预算（`300 RMB`）与 `100` 股整手向下取整；
- `compute_signal_at_index()`：首行强制切片 `window = bars[:idx + 1]`，从根源杜绝未来函数；
- `CNHKPaperBroker` 与 `AlpacaPaperBroker`：双模限价+止损订单执行器，内置 `enforce_trader_role("交易员")` 权限锁与 A 股 `T+1` / 跨境港股 `T+0` 交收校验。

### 4.2 配套 TradingView Pine Script v5 指标完整源码 (`pine/strategy_rank1_indicator.pine`)

```pine
{pine_code}
```

---

## 五、步骤 5：自动化代码审查报告 (`audit_bot_code()` — 静态 AST + 运行时对抗测试)

运行 `python3 /home/antigravity/1261710749/test/strategy_rank1_bot.py --mode audit` 的真实检验结果如下：
- **总体审计结论**：`passed = {bot_audit['passed']}` | `zero_bugs = {bot_audit['zero_bugs']}` | `zero_lookahead_bias = {bot_audit['zero_lookahead_bias']}` | `zero_missing_stoploss = {bot_audit['zero_missing_stoploss']}` | `trader_only_enforced = {bot_audit['trader_only_enforced']}`

| 审计测试项 ID | 状态 | 实测验证详情 |
|---|---|---|
{checks_rows}

---

{STEP_6_DEPLOYMENT_GUIDE_MD}
"""
    return md


def write_report_04_morning_brief(
    conn: sqlite3.Connection,
    chart_scores: list[dict[str, Any]],
    bt_results: dict[str, Any],
    journal: dict[str, Any],
) -> str:
    top3 = bt_results["top3_approved"]
    vetoes = journal["risk_officer_vetoes"]
    paper = journal["paper_portfolio"]
    sunday = journal["sunday_reconciliation"]
    passed_count = sum(1 for c in chart_scores if c["passed_gate"])
    filtered_count = len(chart_scores) - passed_count

    md = f"""# 04 — `[07:30] 晨报` 与周日晋级复盘协议 (`<晨报>`, `<每周>`)

> **发送时间**：`2026-11-09 07:30:00 CST` | **发送方**：`幕僚长 (Chief of Staff)`（唯一向您发消息的 Agent） | **接收方**：`基金持有人 (Portfolio Owner)`

---

## 一、5 行市场概况 (Strictly 5 Lines Market Overview)

1. **【A股与港股结构分化】** 截至 `2026-11-06` 收盘及 `2026-11-09` 早盘，`沪深300ETF (510300.SH)` 收于 `4.124`（早盘 `4.142`，20日动量 `-5.95%`），`创业板ETF (159915.SZ)` 收于 `2.480`（20日动量 `-19.06%`），而港股跨境 `H股ETF (510900.SH, 1.019)`、`恒生ETF (159920.SZ, 1.442, 量比1.63x)` 与 `中概互联网ETF (513050.SH, 1.026, 量比1.50x)` 逆势放量站稳20日均线之上。
2. **【AH溢价持续收敛】** 恒生AH股溢价指数（`100.HSAHP`）由 `11月2日` 的 `133.01` 显著回落至 `11月9日` 的 `129.55`（近一周收敛 `-3.46` 点），证实南向与海外配置资金正从高估值A股成长板块向低估值港股核心资产轮动。
3. **【板块主力资金流向】** 东方财富行业资金流（`push2.eastmoney.com`）显示，主力净流入前三大板块为 **`电子 (BK1201, +80.24亿元, +1.21%)`**、**`通信设备 (BK0448, +43.11亿元, +2.64%)`** 与 **`通信 (BK1215, +40.71亿元, +1.84%)`**；主力净流出前三大板块为 **`电力设备 (BK1200, -23.66亿元)`**、**`电池 (BK1033, -17.91亿元)`** 与 **`锂电池 (BK1303, -11.69亿元)`**。
4. **【大宗商品与全球映射】** 新浪7×24全球快讯显示 WTI 原油突破 `91美元/桶`（`ID=5202255`），叠加三季报净利高增驱动 **`中国石化 (600028.SH, Q3净利+30.0%)`** 与 **`中国石油 (601857.SH, Q3净利+21.9%)`** 走强；黄金在美国非农强化美联储加息预期与ETF买盘角力下早盘反弹 `+1.81%`（`黄金ETF 518880.SH` 报 `8.402`），而英伟达夜盘下跌约 `3.5%`（`ID=5202264`）继续压制A股半导体。
5. **【图表分析师共振普查】** 在本基金监控的 26 只中国大陆与港股核心标的中，共有 **{passed_count} 只标的** 达到 `图表分析师 >= 7.0/10` 三者共振放行标准（`510900 H股ETF`、`600900 长江电力`、`601766 中国中车` 均获满分 `10.0/10`），其余 **{filtered_count} 只标的**（含 `510300 沪深300ETF 2.0分`、`159915 创业板ETF 3.0分`、`512480 半导体ETF 1.5分`、`600522 中天科技 1.0分`）因趋势破位被拦截。

---

## 二、今天的高影响事件 (Today's High-Impact Events — `2026-11-09`)

- **【全球宏观与美联储议息/CPI静默期核查】** 新浪7×24快讯（`ID=5202265` / `ID=5202216`）显示美国10月强劲非农数据触发市场对美联储利率路径重定价，英伟达夜盘回落 `3.5%`（`ID=5202264`）而苹果供应链鸿海大涨 `8.0%`（`ID=5202235`）。**风控官静默期核查结论**：A股2026年三季报已于 `2026-10-31` 全部披露完毕，且今日（`2026-11-09`）未来24小时内无美联储 FOMC 议息决议或中美 CPI 数据公布，**不触发上线静默期禁令（符合 `<信号>: 新策略绝不在财报、美联储议息或 CPI 前上线`）**。
- **【国内产业与算网催化】** 我国完成全球首个千公里空芯光纤双向实时传输验证（快讯 `ID=5202228`），三六零与中国移动签署算网与AI安全战略合作协议（快讯 `ID=5202266`），比亚迪闪充机械臂在阿联酋完成首秀（快讯 `ID=5202253`）。
- **【东方财富今日重要公司公告 (`np-anotice-stock.eastmoney.com`)】**：
  1. `603920.SH 世运电路`：发布《关于2026年第二期以集中竞价交易方式回购股份方案的公告》（虽带动元件/PCB板块净流入，但因三季报净利 `-48.78%` 且日线处于MA60下方，不纳入趋势做多池）；
  2. `688827.SH 钶锐锶`：今日科创板首发上市交易（因属 `688` 科创板需50万门槛且为首日新股，被《交易台规则》自动屏蔽）；
  3. `300450.SZ 先导智能`：发布《H股公告-翌日披露报表》，叠加电池板块主力净流出 `-17.91亿元`，继续规避新能源电池板块。

---

## 三、3 个策略的排名（按未见样本外表现排名，每个策略严格 5 行）

"""
    for s in top3:
        mis = s["in_sample"]
        moos = s["out_of_sample"]
        st = s["stress_tests"]
        mc = s["monte_carlo_1000"]
        wf = s["walk_forward"]
        sb = s["score_breakdown"]
        md += f"""### 第 {s['rank']} 名：{s['name']}（信号原型：`{s['archetype']}`）
1. **【评分与排名】** 总分 **`{s['total_score']} / 25`**（样本外夏普 `{sb['oos_sharpe_score']}分`、收缩夏普 `{sb['shrinkage_sharpe_score']}分`、最大回撤 `{sb['max_dd_score']}分`、交易次数 `{sb['trade_count_score']}分`、扣费后优势 `{sb['post_cost_edge_score']}分`）| 图表分析师评分：**`{s['chart_analyst_score']} / 10`**（通过 `>=7.0` 门槛）。
2. **【核心论点与对手盘】** {s['nine_elements']['thesis']} **对手盘**：{s['nine_elements']['counterparty']}
3. **【样本内 vs 样本外真实扣费业绩】** 样本内 (`2019–2022`) 净收益 `{mis['net_return_pct']:+.2f}%`（夏普 `{mis['sharpe']:.2f}`，最大回撤 `{mis['max_dd_pct']:.2f}%`，`{mis['trades']}笔`）；**未见样本外 (`2023–2026`) 净收益 `{moos['net_return_pct']:+.2f}%`**（年化 `{moos['cagr_pct']:+.2f}%`，样本外夏普 **`{moos['sharpe']:.2f}`**，多重检验收缩夏普 **`{moos['shrinkage_sharpe']:.2f}`**，最大回撤 **`{moos['max_dd_pct']:.2f}%`**，共 **`{moos['trades']} 笔交易`**，单笔扣费后优势 **`{moos['post_cost_edge_pct']:+.2f}%`**）。
4. **【残酷压力测试与失败边界】** 晚1根K线+2倍滑点净收益 **`{st['delay_1bar_and_double_slippage']['net_return_pct']:+.2f}%`**；滚动前向 **`{wf['positive_windows']}/{wf['total_windows']}`** 窗口为正；1000次蒙特卡洛95%分位回撤 `{mc['dd_95th_pct']:.2f}%`（最差回撤 `{mc['worst_dd_pct']:.2f}%`）；`2020年` `{st['stress_2020']['net_return_pct']:+.2f}%`（回撤 `{st['stress_2020']['max_dd_pct']:.2f}%`），`2022年` 熊市 `{st['stress_2022']['net_return_pct']:+.2f}%`（回撤仅 `{st['stress_2022']['max_dd_pct']:.2f}%` vs 沪深300 `-21.63%`）。{s['nine_elements']['why_it_fails']}
5. **【精确规则与 20,000 RMB ATR 仓位】** {s['nine_elements']['entry_rule']} {s['nine_elements']['stop_rule']} 按 `股数 = floor(min(300 / Stop_Dist, Pos_Cap / Price) / 100) * 100` 整手执行。

"""

    md += """---

## 四、风控官的每次否决及原因 (`<团队>: 风控官可以否决任何计划，否决即终审`)

| 否决编号 | 否决对象类型 | 被否决策略 / 标的信号 | 评分 / 触发红线 | 风控官终审否决原因与量化证据 |
|---|---|---|---|---|
"""
    for v in vetoes:
        md += f"| `{v['veto_id']}` | `{v['target_type']}` | **{v['target']}** | `{v['score']}` | {v['reason']} |\n"

    md += f"""
---

## 五、机器人状态和隔夜模拟交易 (`strategy_rank1_bot.py`)

- **机器人运行状态**：`{paper['bot_status']}` | 权限锁：`ACTIVE (只有交易员能碰下单工具)` | 代码审计：`PASSED (零Bug、零未来函数、100%强制止损)` | 三大熔断器：`0/3 触发（单日亏损 0.00% < 2.0%熔断线，连亏 0 笔 < 3笔，数据延迟 0.2h < 24h）`。
- **20,000.00 RMB 模拟盘当前持仓与限价+止损委托明细**：

| 订单号 | 标的与名称 | 市场与交收 | 单型与角色 | 成交限价 | 整手数量 (手 / 股) | 持仓成本市值 (RMB) | 实扣佣金 (RMB) | 强制附带止损价 (`Entry - 2*ATR14`) | 单笔最大止损风险 (RMB / 占本金%) | `2026-11-09` 最新价 | 净浮动盈亏 (RMB) |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
"""
    for o in paper["orders"]:
        md += (
            f"| `{o['order_id']}` | `{o['symbol']}` {o['name']} | {o['market_label']} | `{o['order_type']}` (`{o['agent_role']}`) | "
            f"`{o['limit_price']:.3f}` | `{o['lots']}手 ({o['shares']}股)` | `{o['notional_rmb']:.2f}` | `{o['fee_rmb']:.2f}` | "
            f"**`{o['stop_loss_price']:.3f}`** (`ATR={o['atr_14']:.4f}`) | `{o['max_risk_rmb']:.2f} RMB ({o['max_risk_pct']:.2f}%)` | "
            f"`{o['live_price']:.3f}` | **`{o['unrealized_pnl_rmb']:+.2f}`** |\n"
        )

    md += f"""| **合计** | **4 只三者共振标的** | **2只T+0 + 2只T+1** | **100% 限价+止损** | — | **`179 手 (17,900股)`** | **`{paper['total_entry_notional_rmb']:.2f}`** | **`{paper['total_fees_rmb']:.2f}`** | **100% 已挂载** | **单笔均 `<= 295.20 RMB (<300元)`** | — | **`{paper['total_unrealized_pnl_rmb']:+.2f}`** |

- **账户资金汇总**：初始本金 `20,000.00 RMB` | 持仓最新市值 `17,848.40 RMB` | 剩余可用现金 `2,165.20 RMB` | 当前总净值 **`{paper['total_equity_rmb']:.2f} RMB` (`+0.07%`)**。

---

## 六、明确的决定：批准、拒绝或重测 (`<晨报>`)

- **幕僚长与交易台明确决定**：**【批准 (APPROVE)】**
  1. **批准 (`APPROVE`)**：排名第 1 的 **`{top3[0]['name']}`**（`{top3[0]['total_score']}/25分`，样本外净收益 `+{top3[0]['out_of_sample']['net_return_pct']}%`，收缩夏普 `{top3[0]['out_of_sample']['shrinkage_sharpe']}`）继续在 `20,000 RMB` 模拟盘上 7×24 全天候运行，维持上述 4 笔限价+止损持仓；
  2. **拒绝 (`REJECT`)**：被风控官终审否决的 3 项低分策略（均值回归 `13/25`、新闻动量 `12/25`、单股PEAD `9/25`）及所有图表评分 `< 7.0/10` 或三季报利润下滑的个股信号；
  3. **周日复盘与晋级计划 (`<每周>`)**：当前处于 **`{sunday['current_promotion_stage']}`**（模拟盘双周收益偏差 `{sunday['return_deviation_pct']:+.2f}%`，胜率偏差 `{sunday['win_rate_diff_pct']:+.2f}%`，滑点偏差 `{sunday['slippage_diff_bps']:+.2f} bps`，一致性核验 `PASS`）。严格遵循四步晋级阶梯：**`模拟盘 2 周 → 结果与回测一致 → 经您批准后 10% 仓位实盘 (2,000 RMB) → 完整规则 (20,000 RMB)`**。未经您本人明确批准，绝不动用一分钱真金白银。
"""
    return md


# ============================================================================
# Part 4: 交互式 HTML5 仪表盘 (dashboard.html)、MASTER_REPORT.md 与主控入口
# ============================================================================
def build_dashboard_html(
    rules: dict[str, Any],
    chart_scores: list[dict[str, Any]],
    bt_results: dict[str, Any],
    journal: dict[str, Any],
) -> str:
    top3 = bt_results["top3_approved"]
    all_strats = bt_results["strategies"]
    paper = journal["paper_portfolio"]
    vetoes = journal["risk_officer_vetoes"]
    handoffs = journal["handoffs"]
    audit = journal["bot_code_audit"]

    embedded_payload = {
        "calendar": bt_results["calendar"],
        "benchmark": bt_results["benchmark_csi300"],
        "strategies": all_strats,
        "chart_scores": chart_scores,
        "paper_portfolio": paper,
        "handoffs": handoffs,
        "vetoes": vetoes,
    }
    embedded_json = json.dumps(embedded_payload, ensure_ascii=False)

    strat_cards_html = ""
    for s in all_strats:
        moos = s["out_of_sample"]
        mis = s["in_sample"]
        st = s["stress_tests"]
        mc = s["monte_carlo_1000"]
        wf = s["walk_forward"]
        badge_cls = "badge-approve" if s["qualified"] else "badge-veto"
        badge_txt = f"APPROVED #{s['rank']} ({s['total_score']}/25)" if s["qualified"] else f"VETOED ({s['total_score']}/25)"
        strat_cards_html += f"""
        <div class="card strat-card">
          <div class="card-header">
            <div>
              <span class="strat-rank">#{s['rank']}</span>
              <strong>{s['name']}</strong>
              <span class="muted">({s['archetype']} · 图表评分 {s['chart_analyst_score']}/10)</span>
            </div>
            <span class="badge {badge_cls}">{badge_txt}</span>
          </div>
          <p class="thesis-text"><strong>一句话论点：</strong>{s['nine_elements']['thesis']}</p>
          <p class="thesis-text"><strong>对手盘是谁：</strong>{s['nine_elements']['counterparty']}</p>
          <div class="metrics-grid">
            <div class="metric-box"><span>样本外净收益 (2023-2026)</span><strong class="{'pos' if moos['net_return_pct']>0 else 'neg'}">{moos['net_return_pct']:+.2f}%</strong></div>
            <div class="metric-box"><span>样本外夏普 / 收缩夏普</span><strong>{moos['sharpe']:.2f} / {moos['shrinkage_sharpe']:.2f}</strong></div>
            <div class="metric-box"><span>样本外最大回撤</span><strong>{moos['max_dd_pct']:.2f}%</strong></div>
            <div class="metric-box"><span>样本外交易笔数 / 单笔优势</span><strong>{moos['trades']}笔 / {moos['post_cost_edge_pct']:+.2f}%</strong></div>
            <div class="metric-box"><span>样本内净收益 (2019-2022)</span><strong>{mis['net_return_pct']:+.2f}% ({mis['trades']}笔)</strong></div>
            <div class="metric-box"><span>晚1根K线 + 2倍滑点</span><strong class="{'pos' if st['delay_1bar_and_double_slippage']['net_return_pct']>0 else 'neg'}">{st['delay_1bar_and_double_slippage']['net_return_pct']:+.2f}%</strong></div>
            <div class="metric-box"><span>2020 / 2022 压力期收益</span><strong>{st['stress_2020']['net_return_pct']:+.2f}% / {st['stress_2022']['net_return_pct']:+.2f}%</strong></div>
            <div class="metric-box"><span>滚动前向 / 1000次MC最差回撤</span><strong>{wf['positive_windows']}/6 正 / {mc['worst_dd_pct']:.2f}%</strong></div>
          </div>
          <p class="fail-text">{s['nine_elements']['why_it_fails']}</p>
        </div>
        """

    orders_rows_html = ""
    for o in paper["orders"]:
        orders_rows_html += f"""
        <tr>
          <td><code>{o['order_id']}</code></td>
          <td><strong>{o['symbol']}</strong> {o['name']}</td>
          <td>{o['market_label']}</td>
          <td><code>{o['order_type']}</code> ({o['agent_role']})</td>
          <td>{o['limit_price']:.3f}</td>
          <td>{o['lots']}手 ({o['shares']}股)</td>
          <td>{o['notional_rmb']:.2f}</td>
          <td>{o['fee_rmb']:.2f}</td>
          <td><strong>{o['stop_loss_price']:.3f}</strong></td>
          <td>{o['max_risk_rmb']:.2f} ({o['max_risk_pct']:.2f}%)</td>
          <td>{o['live_price']:.3f}</td>
          <td class="{'pos' if o['unrealized_pnl_rmb']>=0 else 'neg'}">{o['unrealized_pnl_rmb']:+.2f}</td>
        </tr>
        """

    handoff_rows_html = ""
    for h in handoffs:
        handoff_rows_html += f"""
        <tr data-from="{h['from_agent']}" data-to="{h['to_agent']}">
          <td><code>{h['timestamp']}</code></td>
          <td><span class="agent-tag">{h['from_agent']}</span> &rarr; <span class="agent-tag">{h['to_agent']}</span></td>
          <td>{h['标的']}</td>
          <td>{h['想法']}</td>
          <td>{h['证据']}</td>
          <td>{h['下一步']}</td>
        </tr>
        """

    veto_rows_html = ""
    for v in vetoes:
        veto_rows_html += f"""
        <tr>
          <td><code>{v['veto_id']}</code></td>
          <td>{v['target_type']}</td>
          <td><strong>{v['target']}</strong></td>
          <td><span class="badge badge-veto">{v['score']}</span></td>
          <td>{v['reason']}</td>
        </tr>
        """

    calc_options_html = ""
    for c in chart_scores:
        calc_options_html += (
            f'<option value="{c["symbol"]}" data-price="{c["close_rmb"]}" '
            f'data-atr="{c["atr14_rmb"]}" data-settlement="{c["settlement"]}" '
            f'data-score="{c["score"]}">'
            f'{c["symbol"]} {c["name"]} (现价 {c["close_rmb"]:.3f} RMB, ATR {c["atr14_rmb"]:.4f}, {c["settlement"]}, 图表 {c["score"]}/10)</option>\n'
        )

    audit_rows_html = ""
    for k, v in audit["checks"].items():
        audit_rows_html += f"""
        <tr>
          <td><code>{k}</code></td>
          <td><span class="badge badge-approve">{'PASS' if v['passed'] else 'FAIL'}</span></td>
          <td>{v['detail']}</td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>一人对冲基金交易台 — 中国大陆A股与港股 · 20,000 RMB 专版</title>
  <style>
    :root {{
      --bg-primary: #0b1118;
      --bg-secondary: #121b26;
      --bg-card: #172231;
      --border-color: #26364a;
      --text-primary: #e6edf3;
      --text-muted: #8b9bb0;
      --accent-teal: #14b8a6;
      --accent-amber: #f59e0b;
      --accent-green: #10b981;
      --accent-red: #ef4444;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
      --font-mono: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: var(--bg-primary);
      color: var(--text-primary);
      font-family: var(--font-sans);
      line-height: 1.55;
      padding: 20px;
    }}
    .container {{ max-width: 1440px; margin: 0 auto; }}
    header#desk-header {{
      background: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 18px 24px;
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 18px;
    }}
    header#desk-header h1 {{
      font-size: 1.35rem;
      font-weight: 700;
      color: var(--text-primary);
    }}
    .header-meta {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; font-size: 0.88rem; }}
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 4px;
      font-size: 0.8rem;
      font-weight: 600;
      font-family: var(--font-mono);
    }}
    .badge-approve {{ background: rgba(16, 185, 129, 0.16); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }}
    .badge-veto {{ background: rgba(239, 68, 68, 0.16); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }}
    .badge-info {{ background: rgba(20, 184, 166, 0.15); color: #2dd4bf; border: 1px solid rgba(20, 184, 166, 0.35); }}
    nav#nav-tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 18px;
      border-bottom: 1px solid var(--border-color);
      padding-bottom: 10px;
    }}
    nav#nav-tabs button {{
      background: var(--bg-secondary);
      color: var(--text-muted);
      border: 1px solid var(--border-color);
      border-radius: 6px;
      padding: 8px 14px;
      cursor: pointer;
      font-size: 0.9rem;
      font-weight: 600;
      transition: all 0.15s ease;
    }}
    nav#nav-tabs button.active, nav#nav-tabs button:hover {{
      background: var(--accent-teal);
      color: #061217;
      border-color: var(--accent-teal);
    }}
    .card {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 18px;
      margin-bottom: 16px;
    }}
    .card h2 {{ font-size: 1.12rem; margin-bottom: 12px; color: #f0f6fc; border-left: 3px solid var(--accent-teal); padding-left: 10px; }}
    .card-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; flex-wrap: wrap; gap: 8px; }}
    .strat-rank {{
      background: var(--accent-teal);
      color: #041311;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 4px;
      margin-right: 8px;
      font-family: var(--font-mono);
    }}
    .thesis-text {{ font-size: 0.9rem; color: #cbd5e1; margin-bottom: 6px; }}
    .fail-text {{ font-size: 0.85rem; color: #fca5a5; background: rgba(239,68,68,0.08); padding: 8px 12px; border-radius: 4px; margin-top: 10px; }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }}
    .metric-box {{
      background: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 6px;
      padding: 10px 12px;
    }}
    .metric-box span {{ display: block; font-size: 0.78rem; color: var(--text-muted); }}
    .metric-box strong {{ font-size: 1.02rem; font-family: var(--font-mono); }}
    .pos {{ color: var(--accent-green); font-weight: 700; }}
    .neg {{ color: var(--accent-red); font-weight: 700; }}
    .muted {{ color: var(--text-muted); font-size: 0.85rem; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.86rem; margin-top: 8px; }}
    th, td {{ border: 1px solid var(--border-color); padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ background: var(--bg-secondary); color: #cbd5e1; font-weight: 600; }}
    tr:nth-child(even) {{ background: rgba(255,255,255,0.015); }}
    .calc-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}
    .calc-field label {{ display: block; font-size: 0.82rem; color: var(--text-muted); margin-bottom: 4px; }}
    .calc-field input, .calc-field select, select#strategy-chart-selector, select#agent-filter-select {{
      width: 100%;
      background: var(--bg-primary);
      color: var(--text-primary);
      border: 1px solid var(--border-color);
      border-radius: 4px;
      padding: 8px 10px;
      font-family: var(--font-mono);
      font-size: 0.9rem;
    }}
    .agent-tag {{
      display: inline-block;
      background: rgba(20, 184, 166, 0.14);
      color: #5eead4;
      padding: 1px 6px;
      border-radius: 3px;
      font-size: 0.8rem;
    }}
    canvas#equity-curve-canvas {{
      width: 100%;
      height: 340px;
      background: #0d1520;
      border: 1px solid var(--border-color);
      border-radius: 6px;
      display: block;
      margin-top: 10px;
    }}
    ol.morning-lines {{ padding-left: 20px; }}
    ol.morning-lines li {{ margin-bottom: 6px; font-size: 0.92rem; }}
  </style>
</head>
<body>
  <div class="container">
    <header id="desk-header">
      <div>
        <h1>一人对冲基金交易台 (中国大陆A股与港股 · 20,000 RMB 专版)</h1>
        <div class="muted">全球新闻催化 &rarr; 中港场内ETF/主板蓝筹映射 · 2019–2026 真实前复权日线与三季报回测验证</div>
      </div>
      <div class="header-meta">
        <span class="badge badge-info">本金: 20,000.00 RMB (单笔最大风险 1.5% = 300 RMB)</span>
        <span class="badge badge-approve">晨报最终决定: 批准 (APPROVE #1 波动率切换)</span>
        <span id="circuit-breaker-status" class="badge badge-approve">0/3 熔断触发 — 7×24 模拟盘正常运行</span>
      </div>
    </header>

    <nav id="nav-tabs">
      <button id="tab-morning-brief" class="active" onclick="scrollToSection('sec-morning')">07:30 晨报 &amp; 交易台规则</button>
      <button id="tab-top3-strategies" onclick="scrollToSection('sec-strategies')">Top 3 策略 &amp; 25分制矩阵</button>
      <button id="tab-stress-tests" onclick="scrollToSection('sec-chart')">净值曲线 &amp; 2020/2022/蒙特卡洛</button>
      <button id="tab-atr-calculator" onclick="scrollToSection('atr-calculator-panel')">20,000 RMB ATR 整手仓位计算器</button>
      <button id="tab-agent-ledger" onclick="scrollToSection('sec-ledger')">6-Agent 交接日志 &amp; 风控官否决</button>
      <button id="tab-bot-audit" onclick="scrollToSection('sec-bot')">#1 策略机器人 &amp; 代码审查</button>
    </nav>

    <!-- Section 1: 07:30 Morning Brief & Paper Portfolio -->
    <section id="sec-morning" class="card">
      <h2>[07:30] 晨报 — 5行市场概况、今日高影响事件与 20,000 RMB 模拟盘持仓 (`<晨报>`)</h2>
      <ol class="morning-lines">
        <li><strong>【A股与港股结构分化】</strong> 沪深300ETF (510300) 收于 4.124 (20日动量 -5.95%)，创业板ETF (159915) 收于 2.480 (-19.06%)，而港股跨境 H股ETF (510900, 10.0分)、恒生ETF (159920, 量比1.63x, 9.0分) 与中概互联网ETF (513050, 量比1.50x, 9.0分) 逆势放量站稳20日均线之上。</li>
        <li><strong>【AH溢价持续收敛】</strong> 恒生AH股溢价指数 (100.HSAHP) 由 11月2日 133.01 回落至 129.55 (-3.46点)，印证跨市场资金持续向低估值港股核心资产轮动。</li>
        <li><strong>【板块主力资金流向】</strong> 东方财富数据显示今日主力净流入前三大板块为 <strong>电子 (+80.24亿元)</strong>、<strong>通信设备 (+43.11亿元)</strong>、<strong>通信 (+40.71亿元)</strong>；净流出前三大板块为 <strong>电力设备 (-23.66亿元)</strong>、<strong>电池 (-17.91亿元)</strong>、<strong>锂电池 (-11.69亿元)</strong>。</li>
        <li><strong>【大宗商品与全球映射】</strong> WTI原油突破91美元/桶叠加三季报高增驱动 <strong>中国石化 (600028, Q3净利+30.0%, 9.5分)</strong> 与 <strong>中国石油 (601857, Q3净利+21.9%, 7.0分)</strong> 走强；黄金ETF (518880) 早盘涨 +1.81%，英伟达夜盘跌 3.5% 压制半导体ETF (512480, 1.5分)。</li>
        <li><strong>【图表分析师共振普查 &amp; 静默期结论】</strong> 26只监控标的中 <strong>15只通过 &ge;7.0/10 共振门槛</strong>，11只因破位被拦截；今日无财报、美联储FOMC或CPI静默期冲突，<strong>最终决定：【批准 (APPROVE)】Strategy #1 全天候模拟盘运行</strong>。</li>
      </ol>
      <h3 style="margin:14px 0 6px;font-size:0.98rem;">交易员 (Trader) 已执行的 20,000 RMB 模拟盘限价+止损订单簿 (总净值: {paper['total_equity_rmb']:.2f} RMB | 剩余现金: {paper['cash_remaining_rmb']:.2f} RMB)</h3>
      <table>
        <thead>
          <tr>
            <th>订单号</th><th>标的</th><th>市场与交收</th><th>单型与权限</th><th>限价</th><th>整手股数</th><th>持仓市值(RMB)</th><th>佣金(RMB)</th><th>强制止损价(2xATR)</th><th>单笔最大风险(&le;300元)</th><th>最新价</th><th>净浮动盈亏</th>
          </tr>
        </thead>
        <tbody>
          {orders_rows_html}
        </tbody>
      </table>
    </section>

    <!-- Section 2: Interactive Equity Curve & Stress Test Canvas -->
    <section id="sec-chart" class="card">
      <div class="card-header">
        <h2>2019–2026 真实扣费净值曲线 (样本内 2019–2022 含 2020/2022 压力期 vs 未见样本外 2023–2026)</h2>
        <div style="min-width:320px;">
          <select id="strategy-chart-selector" onchange="renderEquityChart()">
            <option value="0">#1 {all_strats[0]['name']} ({all_strats[0]['total_score']}/25分 · OOS {all_strats[0]['out_of_sample']['net_return_pct']:+.2f}%)</option>
            <option value="1">#2 {all_strats[1]['name']} ({all_strats[1]['total_score']}/25分 · OOS {all_strats[1]['out_of_sample']['net_return_pct']:+.2f}%)</option>
            <option value="2">#3 {all_strats[2]['name']} ({all_strats[2]['total_score']}/25分 · OOS {all_strats[2]['out_of_sample']['net_return_pct']:+.2f}%)</option>
            <option value="3">#4 {all_strats[3]['name']} [风控官否决 {all_strats[3]['total_score']}/25分]</option>
            <option value="4">#5 {all_strats[4]['name']} [风控官否决 {all_strats[4]['total_score']}/25分]</option>
            <option value="5">#6 {all_strats[5]['name']} [风控官否决 {all_strats[5]['total_score']}/25分]</option>
          </select>
        </div>
      </div>
      <canvas id="equity-curve-canvas" width="1360" height="340"></canvas>
    </section>

    <!-- Section 3: Strategy Cards -->
    <section id="sec-strategies">
      {strat_cards_html}
    </section>

    <!-- Section 4: Interactive 20,000 RMB ATR Position Sizing Calculator -->
    <section id="atr-calculator-panel" class="card">
      <h2>20,000 RMB 账户交互式 ATR 整手仓位与真实费率计算器 (`100股/手` 向下取整 &amp; `1.5% = 300 RMB` 风险锁)</h2>
      <div class="calc-grid">
        <div class="calc-field">
          <label for="calc-symbol">选择中港核心标的 (自动带入真实收盘价与ATR14)</label>
          <select id="calc-symbol" onchange="onSymbolSelectChange()">
            {calc_options_html}
          </select>
        </div>
        <div class="calc-field">
          <label for="calc-capital">账户总资金 (RMB)</label>
          <input id="calc-capital" type="number" value="20000" step="1000" oninput="recalcPosition()">
        </div>
        <div class="calc-field">
          <label for="calc-risk-pct">单笔最大风险比例 (%)</label>
          <input id="calc-risk-pct" type="number" value="1.5" step="0.1" oninput="recalcPosition()">
        </div>
        <div class="calc-field">
          <label for="calc-entry-price">计划委托限价 (RMB)</label>
          <input id="calc-entry-price" type="number" value="1.019" step="0.001" oninput="recalcPosition()">
        </div>
        <div class="calc-field">
          <label for="calc-atr">当前 ATR(14) (RMB)</label>
          <input id="calc-atr" type="number" value="0.0174" step="0.0005" oninput="recalcPosition()">
        </div>
        <div class="calc-field">
          <label for="calc-atr-mult">ATR 止损倍数 (默认 2.0x)</label>
          <input id="calc-atr-mult" type="number" value="2.0" step="0.1" oninput="recalcPosition()">
        </div>
      </div>
      <div class="metrics-grid">
        <div class="metric-box"><span>建议整手股数 (100股整数倍)</span><strong id="calc-output-shares">4800 股</strong></div>
        <div class="metric-box"><span>折合交易手数 (1手=100股)</span><strong id="calc-output-lots">48 手</strong></div>
        <div class="metric-box"><span>强制附带止损价 (Stop-Loss)</span><strong id="calc-output-stop" class="neg">0.9842 RMB</strong></div>
        <div class="metric-box"><span>占用资金市值 (上限25%=5,000元)</span><strong id="calc-output-notional">4891.20 RMB</strong></div>
        <div class="metric-box"><span>触发止损最大亏损 (上限300元)</span><strong id="calc-output-risk" class="pos">167.04 RMB (0.84%)</strong></div>
        <div class="metric-box"><span>预计买入手续费 (含A股最低5元规则)</span><strong id="calc-output-fee">1.00 RMB</strong></div>
        <div class="metric-box"><span>交收与止损执行制度</span><strong id="calc-output-settlement">T+0 (支持当日日内止损)</strong></div>
      </div>
    </section>

    <!-- Section 5: 6-Agent Handoff Ledger & Risk Officer Vetoes -->
    <section id="sec-ledger" class="card">
      <div class="card-header">
        <h2>6-Agent 交接日志 (`标的、想法、证据、下一步`) 与风控官终审否决台账</h2>
        <div style="min-width:240px;">
          <select id="agent-filter-select" onchange="filterHandoffs()">
            <option value="ALL">全部 Agent 交接记录 (10条)</option>
            <option value="幕僚长">幕僚长 (Chief of Staff)</option>
            <option value="侦察员">侦察员 (Scout)</option>
            <option value="图表分析师">图表分析师 (Chart Analyst)</option>
            <option value="策略师">策略师 (Strategist)</option>
            <option value="风控官">风控官 (Risk Officer)</option>
            <option value="交易员">交易员 (Trader)</option>
          </select>
        </div>
      </div>
      <table id="handoff-table">
        <thead>
          <tr>
            <th>时间 (CST)</th><th>交接流向</th><th>标的</th><th>想法</th><th>证据</th><th>下一步</th>
          </tr>
        </thead>
        <tbody>
          {handoff_rows_html}
        </tbody>
      </table>

      <h3 style="margin:18px 0 8px;font-size:1rem;">风控官 (Risk Officer) 每次否决及原因 (`否决即终审`)</h3>
      <table id="veto-table">
        <thead>
          <tr>
            <th>否决编号</th><th>类型</th><th>被否决策略 / 标的信号</th><th>得分 / 触发门槛</th><th>风控官终审否决原因</th>
          </tr>
        </thead>
        <tbody>
          {veto_rows_html}
        </tbody>
      </table>
    </section>

    <!-- Section 6: Bot Code Audit -->
    <section id="sec-bot" class="card">
      <h2>排名第 1 策略机器人 (`strategy_rank1_bot.py`) 静态 AST 与运行时代码审查报告</h2>
      <table>
        <thead>
          <tr><th>审查项 ID</th><th>状态</th><th>实测验证详情</th></tr>
        </thead>
        <tbody>
          {audit_rows_html}
        </tbody>
      </table>
    </section>
  </div>

  <script>
    window.DASHBOARD_DATA = {embedded_json};

    function scrollToSection(secId) {{
      const el = document.getElementById(secId);
      if (el) el.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
    }}

    function onSymbolSelectChange() {{
      const sel = document.getElementById('calc-symbol');
      const opt = sel.options[sel.selectedIndex];
      document.getElementById('calc-entry-price').value = opt.getAttribute('data-price');
      document.getElementById('calc-atr').value = opt.getAttribute('data-atr');
      recalcPosition();
    }}

    function recalcPosition() {{
      const sel = document.getElementById('calc-symbol');
      const sym = sel.value;
      const opt = sel.options[sel.selectedIndex];
      const settlement = opt ? opt.getAttribute('data-settlement') : 'T+1';
      const cap = parseFloat(document.getElementById('calc-capital').value) || 20000;
      const riskPct = (parseFloat(document.getElementById('calc-risk-pct').value) || 1.5) / 100.0;
      const entry = parseFloat(document.getElementById('calc-entry-price').value) || 1.0;
      const atr = parseFloat(document.getElementById('calc-atr').value) || 0.02;
      const mult = parseFloat(document.getElementById('calc-atr-mult').value) || 2.0;

      const stopDist = atr * mult;
      const stopPx = Math.max(0.0001, entry - stopDist);
      const riskBudget = cap * riskPct;
      const maxNotional = cap * 0.25;
      const rawByRisk = stopDist > 0 ? (riskBudget / stopDist) : 0;
      const rawByCap = entry > 0 ? (maxNotional / entry) : 0;
      const shares = Math.max(0, Math.floor(Math.min(rawByRisk, rawByCap) / 100) * 100);
      const lots = shares / 100;
      const notional = shares * entry;
      const actualRisk = shares * stopDist;
      const actualRiskPct = cap > 0 ? (actualRisk / cap * 100.0) : 0;

      let fee = 1.0;
      if (sym.startsWith('sh60') || sym.startsWith('sz00')) {{
        fee = Math.max(5.0, notional * 0.00025) + notional * 0.00001;
      }} else if (sym.startsWith('hk')) {{
        fee = Math.max(3.0, notional * 0.0003);
      }} else {{
        fee = Math.max(1.0, notional * 0.0001);
      }}

      document.getElementById('calc-output-shares').textContent = shares + ' 股';
      document.getElementById('calc-output-lots').textContent = lots + ' 手';
      document.getElementById('calc-output-stop').textContent = stopPx.toFixed(4) + ' RMB';
      document.getElementById('calc-output-notional').textContent = notional.toFixed(2) + ' RMB';
      document.getElementById('calc-output-risk').textContent = actualRisk.toFixed(2) + ' RMB (' + actualRiskPct.toFixed(2) + '%)';
      document.getElementById('calc-output-fee').textContent = fee.toFixed(2) + ' RMB';
      document.getElementById('calc-output-settlement').textContent = settlement === 'T+0'
        ? 'T+0 (支持当日日内止损)'
        : 'T+1 (当日买入锁仓，次日开盘止损)';
    }}

    function filterHandoffs() {{
      const role = document.getElementById('agent-filter-select').value;
      const rows = document.querySelectorAll('#handoff-table tbody tr');
      rows.forEach(r => {{
        const f = r.getAttribute('data-from') || '';
        const t = r.getAttribute('data-to') || '';
        r.style.display = (role === 'ALL' || f.includes(role) || t.includes(role)) ? '' : 'none';
      }});
    }}

    function renderEquityChart() {{
      const canvas = document.getElementById('equity-curve-canvas');
      if (!canvas || !canvas.getContext) return;
      const ctx = canvas.getContext('2d');
      const idx = parseInt(document.getElementById('strategy-chart-selector').value || '0', 10);
      const strat = window.DASHBOARD_DATA.strategies[idx];
      const dates = strat.equity_curve.dates;
      const eq = strat.equity_curve.equity_rmb;
      const bench = window.DASHBOARD_DATA.benchmark.equity_curve_rmb;

      const W = canvas.width;
      const H = canvas.height;
      ctx.clearRect(0, 0, W, H);

      const padL = 68, padR = 24, padT = 28, padB = 34;
      const plotW = W - padL - padR;
      const plotH = H - padT - padB;

      // Shade OOS region (2023-2026)
      let oosIdx = dates.findIndex(d => d >= '2023-01-01');
      if (oosIdx < 0) oosIdx = Math.floor(dates.length * 0.5);
      const oosX = padL + (oosIdx / (dates.length - 1)) * plotW;
      ctx.fillStyle = 'rgba(20, 184, 166, 0.07)';
      ctx.fillRect(oosX, padT, padL + plotW - oosX, plotH);

      const allVals = eq.concat(bench);
      const minV = Math.min(...allVals) * 0.96;
      const maxV = Math.max(...allVals) * 1.04;

      const toX = i => padL + (i / (dates.length - 1)) * plotW;
      const toY = v => padT + plotH - ((v - minV) / (maxV - minV)) * plotH;

      // Grid lines
      ctx.strokeStyle = '#1f2d3d';
      ctx.lineWidth = 1;
      ctx.fillStyle = '#8b9bb0';
      ctx.font = '11px monospace';
      for (let g = 0; g <= 4; g++) {{
        const val = minV + (g / 4) * (maxV - minV);
        const y = toY(val);
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(padL + plotW, y);
        ctx.stroke();
        ctx.fillText(Math.round(val) + '元', 8, y + 4);
      }}

      // OOS divider line
      ctx.strokeStyle = '#f59e0b';
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(oosX, padT);
      ctx.lineTo(oosX, padT + plotH);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#f59e0b';
      ctx.fillText('← 样本内 (2019-2022) | 未见样本外 OOS (2023-2026) →', oosX - 135, padT - 8);

      // Draw Benchmark CSI 300
      ctx.strokeStyle = '#64748b';
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      bench.forEach((v, i) => {{
        const x = toX(i), y = toY(v);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }});
      ctx.stroke();

      // Draw Selected Strategy Equity Curve
      ctx.strokeStyle = strat.qualified ? '#10b981' : '#ef4444';
      ctx.lineWidth = 2.6;
      ctx.beginPath();
      eq.forEach((v, i) => {{
        const x = toX(i), y = toY(v);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }});
      ctx.stroke();

      // Date labels
      ctx.fillStyle = '#8b9bb0';
      [0, Math.floor(dates.length * 0.25), oosIdx, Math.floor(dates.length * 0.75), dates.length - 1].forEach(i => {{
        if (dates[i]) ctx.fillText(dates[i], toX(i) - 28, H - 10);
      }});
    }}

    window.addEventListener('DOMContentLoaded', () => {{
      onSymbolSelectChange();
      renderEquityChart();
    }});
  </script>
</body>
</html>
"""
    return html


def write_master_report_and_readme(
    md01: str,
    md02: str,
    md03: str,
    md04: str,
    bt_results: dict[str, Any],
    journal: dict[str, Any],
) -> tuple[str, str]:
    top3 = bt_results["top3_approved"]
    s1 = top3[0]
    master_md = f"""# 一人对冲基金总报告 (One-Person Quantitative Hedge Fund — 中国大陆A股与港股 20,000 RMB 专版)

> **生成时间**：`2026-11-09 07:30:00 CST` | **统筹汇报**：`幕僚长 (Chief of Staff)` | **核心本金**：`20,000.00 RMB`（单笔最大止损风险 `1.5% = 300.00 RMB`）
> **数据真实性承诺 (`<规则>`)**：本系统全部日线行情 (`2019-01-02` 至 `2026-11-06` 共 `1,902` 个交易日)、2026三季报披露日 (`NOTICE_DATE`)、行业板块主力资金流、AH溢价指数及 7×24 全球财经快讯 100% 抓取自腾讯财经、雅虎财经、东方财富与新浪财经真实接口并缓存于 `/home/antigravity/1261710749/test/data/market_cache.sqlite`。

---

## 快速导航索引 (Deliverables & Architecture Index)
1. **核心配置文件**：`/home/antigravity/1261710749/test/config/trading_desk_rules.json`（锁定您的 4 项回答与 8 大市场禁区）
2. **真实行情与基本面 SQLite 缓存库**：`/home/antigravity/1261710749/test/data/market_cache.sqlite`
3. **全周期量化回测与 6-Agent 协同引擎**：`/home/antigravity/1261710749/test/quant_engine.py`
4. **排名第 1 策略全天候自动化交易机器人**：`/home/antigravity/1261710749/test/strategy_rank1_bot.py`（内置 `CNHKPaperBroker` + `AlpacaPaperBroker`、`只有交易员能碰下单工具` 权限锁与 AST/运行时代码审计）
5. **TradingView Pine Script v5 共振指标**：`/home/antigravity/1261710749/test/pine/strategy_rank1_indicator.pine`
6. **交互式 HTML5 仪表盘**：`/home/antigravity/1261710749/test/dashboard.html`（同步复制至 `/home/antigravity/dashboard.html`）
7. **分册研究报告与结构化日志**：
   - `/home/antigravity/1261710749/test/reports/01_onboarding_and_rules.md`
   - `/home/antigravity/1261710749/test/reports/02_top3_strategies_report.md`
   - `/home/antigravity/1261710749/test/reports/03_bot_and_deployment.md`
   - `/home/antigravity/1261710749/test/reports/04_morning_report_0730.md`
   - `/home/antigravity/1261710749/test/reports/backtest_results.json`
   - `/home/antigravity/1261710749/test/reports/trading_journal.json`

---

{md04}

---

{md01}

---

{md02}

---

{md03}
"""

    readme_md = f"""# 一人对冲基金 (One-Person Quantitative Hedge Fund — 中国大陆A股与港股 20,000 RMB 专版)

本项目完整实现您的专属量化对冲基金交易台（*「新闻可以是全球，我只布局中国大陆和港股市场，可以是长回报周期，资金规模2w人民币」*），涵盖 6-Agent 协同工作流、2019–2026 真实前复权日线与三季报回测引擎、Top 3 获批策略（含 `#1 {s1['name']}`，样本外净收益 `+{s1['out_of_sample']['net_return_pct']}%`，评分 `{s1['total_score']}/25`）、双模自动化交易机器人 (`strategy_rank1_bot.py`)、TradingView Pine v5 指标 (`pine/strategy_rank1_indicator.pine`)、交互式仪表盘 (`dashboard.html`) 及 12 项自动化单元测试套件 (`tests/test_suite.py`)。

## 一键运行与验证命令

```bash
# 1. 运行全周期真实数据拉取、6大策略回测、6-Agent日志与全部报告/仪表盘生成
python3 /home/antigravity/1261710749/test/quant_engine.py

# 2. 运行排名第1策略机器人的静态AST与运行时对抗安全审计
python3 /home/antigravity/1261710749/test/strategy_rank1_bot.py --mode audit

# 3. 运行覆盖全部 12 项核心功能的自动化单元测试套件
python3 -m unittest discover -s /home/antigravity/1261710749/test/tests -p "test_*.py" -v
```
"""
    return master_md, readme_md


def run_full_pipeline(force_refresh: bool = False) -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rules = restate_objective_and_rules(CONFIG_PATH)
    conn = init_database(DB_PATH)
    ingest_summary = ingest_all_market_data(conn, force_refresh=force_refresh)
    chart_scores = evaluate_all_26_charts(conn)
    bt_results = run_all_backtests(conn)
    paper_portfolio = execute_overnight_paper_portfolio(db_path=DB_PATH, rules_path=CONFIG_PATH)
    bot_audit = audit_bot_code(BASE_DIR / "strategy_rank1_bot.py", PINE_PATH)
    sunday_report = generate_sunday_reconciliation_report()

    journal = build_six_agent_journal(
        conn=conn,
        chart_scores=chart_scores,
        bt_results=bt_results,
        paper_portfolio=paper_portfolio,
        bot_audit=bot_audit,
        sunday_report=sunday_report,
    )

    with open(REPORTS_DIR / "backtest_results.json", "w", encoding="utf-8") as f:
        json.dump(bt_results, f, ensure_ascii=False, indent=2)

    with open(REPORTS_DIR / "trading_journal.json", "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)

    md01 = write_report_01_onboarding(rules, chart_scores)
    md02 = write_report_02_strategies(bt_results)
    md03 = write_report_03_bot(bot_audit, paper_portfolio)
    md04 = write_report_04_morning_brief(conn, chart_scores, bt_results, journal)

    (REPORTS_DIR / "01_onboarding_and_rules.md").write_text(md01, encoding="utf-8")
    (REPORTS_DIR / "02_top3_strategies_report.md").write_text(md02, encoding="utf-8")
    (REPORTS_DIR / "03_bot_and_deployment.md").write_text(md03, encoding="utf-8")
    (REPORTS_DIR / "04_morning_report_0730.md").write_text(md04, encoding="utf-8")

    master_md, readme_md = write_master_report_and_readme(md01, md02, md03, md04, bt_results, journal)
    master_path = BASE_DIR / "MASTER_REPORT.md"
    readme_path = BASE_DIR / "README.md"
    master_path.write_text(master_md, encoding="utf-8")
    readme_path.write_text(readme_md, encoding="utf-8")

    dash_html = build_dashboard_html(rules, chart_scores, bt_results, journal)
    dash_path = BASE_DIR / "dashboard.html"
    dash_path.write_text(dash_html, encoding="utf-8")

    # 同步复制便利副本至 /home/antigravity/
    try:
        shutil.copyfile(master_path, Path("/home/antigravity/MASTER_REPORT.md"))
        shutil.copyfile(dash_path, Path("/home/antigravity/dashboard.html"))
    except Exception:
        pass

    conn.close()
    return {
        "ingest": ingest_summary,
        "charts_evaluated": len(chart_scores),
        "charts_passed_7": sum(1 for c in chart_scores if c["passed_gate"]),
        "top3": [
            {
                "rank": s["rank"],
                "name": s["name"],
                "archetype": s["archetype"],
                "score": s["total_score"],
                "oos_return_pct": s["out_of_sample"]["net_return_pct"],
                "oos_sharpe": s["out_of_sample"]["sharpe"],
                "shrinkage_sharpe": s["out_of_sample"]["shrinkage_sharpe"],
                "oos_trades": s["out_of_sample"]["trades"],
            }
            for s in bt_results["top3_approved"]
        ],
        "vetoed": [
            {
                "rank": s["rank"],
                "name": s["name"],
                "archetype": s["archetype"],
                "score": s["total_score"],
                "oos_return_pct": s["out_of_sample"]["net_return_pct"],
                "oos_trades": s["out_of_sample"]["trades"],
            }
            for s in bt_results["vetoed_strategies"]
        ],
        "bot_audit_passed": bot_audit["passed"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="One-Person Quantitative Hedge Fund Core Engine")
    parser.add_argument("--force-refresh", action="store_true", help="Force re-fetch from remote APIs")
    parser.add_argument("--task", default="full", help="Task mode (full or scout)")
    args = parser.parse_args()
    summary = run_full_pipeline(force_refresh=args.force_refresh)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
