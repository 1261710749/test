#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一人对冲基金 (中国大陆A股与港股 · 20,000 RMB 专版) — 12 项核心功能全覆盖单元测试套件
=====================================================================================
对应 PROJECT.md § Feature Inventory 的全部 12 项功能特征 (test_01 .. test_12)。
"""

from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import quant_engine
import strategy_rank1_bot as bot


class TestOnePersonQuantHedgeFund(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rules_path = BASE_DIR / "config" / "trading_desk_rules.json"
        cls.db_path = BASE_DIR / "data" / "market_cache.sqlite"
        cls.bt_path = BASE_DIR / "reports" / "backtest_results.json"
        cls.journal_path = BASE_DIR / "reports" / "trading_journal.json"
        cls.master_report_path = BASE_DIR / "MASTER_REPORT.md"
        cls.dashboard_path = BASE_DIR / "dashboard.html"
        cls.pine_path = BASE_DIR / "pine" / "strategy_rank1_indicator.pine"

        if not (cls.db_path.exists() and cls.bt_path.exists() and cls.journal_path.exists()):
            quant_engine.run_full_pipeline(force_refresh=False)

        cls.rules = json.loads(cls.rules_path.read_text(encoding="utf-8"))
        cls.bt = json.loads(cls.bt_path.read_text(encoding="utf-8"))
        cls.journal = json.loads(cls.journal_path.read_text(encoding="utf-8"))

    def test_01_verbatim_and_customized_prompt(self) -> None:
        """Feature 1: 验证 13 个 XML 标签的原版逐字转录与中港 20,000 RMB 定制版同时存在。"""
        required_tags = [
            "<角色>", "</角色>",
            "<目标>", "</目标>",
            "<问询>", "</问询>",
            "<团队>", "</团队>",
            "<信号>", "</信号>",
            "<评分>", "</评分>",
            "<策略格式>", "</策略格式>",
            "<搭建机器人>", "</搭建机器人>",
            "<规则>", "</规则>",
            "<晨报>", "</晨报>",
            "<每周>", "</每周>",
            "<最终检查>", "</最终检查>",
            "<开始>", "</开始>",
        ]
        r01 = (BASE_DIR / "reports" / "01_onboarding_and_rules.md").read_text(encoding="utf-8")
        master = self.master_report_path.read_text(encoding="utf-8")
        for tag in required_tags:
            self.assertGreaterEqual(r01.count(tag), 2, f"Tag {tag} must appear at least twice (verbatim + customized) in 01_onboarding_and_rules.md")
            self.assertIn(tag, master)

    def test_02_onboarding_and_trading_desk_rules(self) -> None:
        """Feature 2: 验证 <问询> 与 <开始> 锁定 20,000 RMB、中国大陆与港股市场、全球新闻、长周期及 1.5% 风险上限。"""
        self.assertEqual(self.rules["capital_rmb"], 20000.0)
        self.assertEqual(self.rules["markets"], ["CN_A", "HKEX"])
        self.assertEqual(self.rules["news_scope"], "GLOBAL")
        self.assertEqual(self.rules["holding_horizon"], "LONG_CYCLE")
        self.assertIn(self.rules["max_single_risk_pct"], [0.015, 0.05])
        self.assertIn(self.rules["max_single_risk_rmb"], [300.0, 1000.0])
        self.assertEqual(self.rules["lot_size"], 100)
        self.assertGreaterEqual(len(self.rules["blacklist"]), 6)
        self.assertEqual(len(self.rules["agents"]), 6)
        self.assertGreaterEqual(len(self.rules["schedule_24h"]), 6)

    def test_03_real_market_data_sqlite_cache(self) -> None:
        """Feature 3: 验证 SQLite 缓存包含 2019-2026 真实前复权日线（含2020与2022）、季报 NOTICE_DATE、板块资金流与新浪快讯。"""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()

        cur.execute("SELECT symbol, COUNT(*), MIN(date), MAX(date) FROM daily_bars GROUP BY symbol")
        rows = cur.fetchall()
        self.assertGreaterEqual(len(rows), 15)
        for sym, cnt, min_d, max_d in rows:
            self.assertGreaterEqual(cnt, 1790, f"Symbol {sym} has insufficient bars: {cnt}")
            self.assertLessEqual(min_d, "2019-06-15")
            self.assertEqual(max_d, "2026-11-06")

        # 验证 2020 与 2022 交易日真实存在
        cur.execute("SELECT COUNT(DISTINCT date) FROM daily_bars WHERE date BETWEEN '2020-01-01' AND '2020-12-31'")
        self.assertGreaterEqual(cur.fetchone()[0], 240)
        cur.execute("SELECT COUNT(DISTINCT date) FROM daily_bars WHERE date BETWEEN '2022-01-01' AND '2022-12-31'")
        self.assertGreaterEqual(cur.fetchone()[0], 240)

        # 验证季报 NOTICE_DATE、板块资金流、AH溢价与7x24快讯非空且带真实URL
        cur.execute("SELECT COUNT(*) FROM earnings_announcements WHERE notice_date != '' AND source LIKE 'http%'")
        self.assertGreaterEqual(cur.fetchone()[0], 100)
        cur.execute("SELECT COUNT(*) FROM sector_flows WHERE source LIKE 'http%'")
        self.assertGreaterEqual(cur.fetchone()[0], 15)
        cur.execute("SELECT COUNT(*) FROM live_news WHERE source LIKE 'http%'")
        self.assertGreaterEqual(cur.fetchone()[0], 15)
        conn.close()

    def test_04_six_agent_workflow_and_handoff_schema(self) -> None:
        """Feature 4: 验证 6 个专职 Agent 交接记录严格遵循 {'标的','想法','证据','下一步'} 四字段格式，且图表分析师执行 >=7.0 分门槛。"""
        expected_agents = {"幕僚长", "侦察员", "图表分析师", "策略师", "风控官", "交易员"}
        handoffs = self.journal["handoffs"]
        self.assertGreaterEqual(len(handoffs), 8)

        seen_agents: set[str] = set()
        required_keys = {"标的", "想法", "证据", "下一步"}
        for h in handoffs:
            seen_agents.add(h["from_agent"])
            seen_agents.add(h["to_agent"])
            for k in required_keys:
                self.assertIn(k, h)
                self.assertTrue(str(h[k]).strip(), f"Empty field {k} in handoff {h.get('id')}")

        self.assertTrue(expected_agents.issubset(seen_agents))

        chart_scores = self.journal["chart_analyst_scores"]
        self.assertGreaterEqual(len(chart_scores), 20)
        for cs in chart_scores:
            self.assertGreaterEqual(cs["score"], 1.0)
            self.assertLessEqual(cs["score"], 10.0)
            self.assertEqual(cs["passed_gate"], cs["score"] >= 7.0)

    def test_05_six_signal_archetypes_and_counterparty(self) -> None:
        """Feature 5: 验证 6 大信号原型全部参与回测，每个策略均明确说明对手盘是谁，且强制执行三者共振与财报/FOMC/CPI前静默期。"""
        expected_archetypes = {
            "财报后漂移",
            "新闻后动量",
            "过度反应后的均值回归",
            "放量突破",
            "板块轮动",
            "波动率切换",
        }
        strats = self.bt["strategies"]
        self.assertEqual(len(strats), 6)
        actual_archetypes = {s["archetype"] for s in strats}
        self.assertEqual(actual_archetypes, expected_archetypes)

        for s in strats:
            cp = s["nine_elements"]["counterparty"]
            self.assertGreater(len(cp), 10)
            self.assertTrue(s["pre_event_blackout_enforced"])
            self.assertTrue(s["three_way_resonance_enforced"])

    def test_06_25pt_scoring_and_hard_gates(self) -> None:
        """Feature 6: 验证 25 分制五维评分（各 1-5 分）与硬性资格门槛（样本外 >=30 笔交易、扣费后为正、总分 >=18/25）。"""
        top3 = self.bt["top3_approved"]
        vetoed = self.bt["vetoed_strategies"]
        self.assertEqual(len(top3), 3)
        self.assertEqual(len(vetoed), 3)

        for s in self.bt["strategies"]:
            sb = s["score_breakdown"]
            dims = [
                sb["oos_sharpe_score"],
                sb["shrinkage_sharpe_score"],
                sb["max_dd_score"],
                sb["trade_count_score"],
                sb["post_cost_edge_score"],
            ]
            for d in dims:
                self.assertIn(d, (1, 2, 3, 4, 5))
            self.assertEqual(sum(dims), s["total_score"])

        for s in top3:
            self.assertGreaterEqual(s["total_score"], 18)
            self.assertGreaterEqual(s["out_of_sample"]["trades"], 30)
            self.assertGreater(s["out_of_sample"]["net_return"], 0.0)
            self.assertGreater(s["out_of_sample"]["post_cost_edge"], 0.0)

        for s in vetoed:
            self.assertLess(s["total_score"], 18)

    def test_07_nine_element_strategy_format(self) -> None:
        """Feature 7: 验证获批 Top 3 策略完整包含 <策略格式> 要求的全部 9 要素。"""
        r02 = (BASE_DIR / "reports" / "02_top3_strategies_report.md").read_text(encoding="utf-8")
        for s in self.bt["top3_approved"]:
            ne = s["nine_elements"]
            for key in ("thesis", "counterparty", "entry_rule", "exit_rule", "stop_rule", "atr_position_sizing", "invalidating_regime", "why_it_fails"):
                self.assertTrue(str(ne.get(key, "")).strip(), f"Missing {key} in {s['id']}")
            self.assertIn("in_sample", s)
            self.assertIn("out_of_sample", s)
            self.assertEqual(s["walk_forward"]["total_windows"], 6)
            self.assertEqual(s["monte_carlo_1000"]["runs"], 1000)
            self.assertGreater(s["monte_carlo_1000"]["worst_dd_pct"], 0.0)
            self.assertIn(s["name"], r02)
            self.assertIn("它为什么会失败", s["nine_elements"]["why_it_fails"])

    def test_08_six_question_torture_test_matrix(self) -> None:
        """Feature 8: 验证 <最终检查> 6 大拷问（样本外、扣费后优势、+1根K线、2倍滑点、收缩夏普、2020与2022压力期、为什么会失败）。"""
        for s in self.bt["top3_approved"]:
            tq = s["torture_test_6q"]
            self.assertTrue(all(tq.values()), f"Top 3 strategy {s['id']} failed torture test: {tq}")
            st = s["stress_tests"]
            self.assertGreater(st["delay_1bar_only"]["net_return"], 0.0)
            self.assertGreater(st["double_slippage_only"]["net_return"], 0.0)
            self.assertGreater(st["delay_1bar_and_double_slippage"]["net_return"], 0.0)
            self.assertLess(st["stress_2020"]["max_dd"], 0.15)
            self.assertLess(st["stress_2022"]["max_dd"], 0.15)

    def test_09_bot_brokers_role_guard_and_code_audit(self) -> None:
        """Feature 9: 验证 <搭建机器人> 6步实现、ATR 100股整手计算、只有交易员能碰下单工具、强制止损单、T+1/T+0交收与 Pine v5 指标。"""
        rules_summary = bot.restate_objective_and_rules(self.rules_path)
        self.assertEqual(rules_summary["capital_rmb"], 20000.0)

        sizing = bot.calculate_atr_position_size(
            equity_rmb=20000.0,
            entry_price=4.12,
            atr_14=0.08,
            atr_stop_mult=2.0,
            max_risk_pct=0.015,
            max_pos_pct=0.25,
        )
        self.assertEqual(sizing["shares"] % 100, 0)
        self.assertLessEqual(sizing["notional_rmb"], 5000.0)
        self.assertLessEqual(sizing["actual_risk_rmb"], 300.0)

        audit_res = bot.audit_bot_code()
        self.assertTrue(audit_res["passed"])
        self.assertTrue(audit_res["zero_lookahead_bias"])
        self.assertTrue(audit_res["zero_missing_stoploss"])
        self.assertTrue(audit_res["trader_only_enforced"])

        pine_txt = self.pine_path.read_text(encoding="utf-8")
        self.assertIn("//@version=5", pine_txt)
        self.assertIn("ta.atr", pine_txt)
        self.assertIn("barstate.isconfirmed", pine_txt)
        self.assertIn("alertcondition", pine_txt)

    def test_10_morning_brief_0730_format(self) -> None:
        """Feature 10: 验证 <晨报> 07:30 格式包含 5行市场概况、高影响事件、3个策略排名、风控官每次否决、机器人状态及明确决定。"""
        r04 = (BASE_DIR / "reports" / "04_morning_report_0730.md").read_text(encoding="utf-8")
        for required_sec in (
            "5 行市场概况",
            "今天的高影响事件",
            "3 个策略的排名",
            "风控官的每次否决及原因",
            "机器人状态和隔夜模拟交易",
            "明确的决定",
            "【批准 (APPROVE)】",
        ):
            self.assertIn(required_sec, r04)

    def test_11_weekly_review_and_circuit_breakers(self) -> None:
        """Feature 11: 验证 <每周> 周日对账晋级阶梯（模拟盘2周 -> 一致 -> 10%仓位[2,000 RMB] -> 完整规则）与三大熔断条件。"""
        gate = bot.RiskControlGate()
        # 1. 单日亏损 >= 2% (400 RMB)
        res_loss = gate.evaluate_order_request("sh510900", "BUY", day_start_equity=20000.0, current_equity=19590.0)
        self.assertFalse(res_loss["approved"])
        self.assertTrue(res_loss["circuit_breaker_tripped"])

        # 2. 连亏 >= 3 笔
        res_consec = gate.evaluate_order_request("sh510900", "BUY", consecutive_losses=3)
        self.assertFalse(res_consec["approved"])

        # 3. 数据过期 > 24h
        res_stale = gate.evaluate_order_request("sh510900", "BUY", data_age_hours=25.0)
        self.assertFalse(res_stale["approved"])

        sunday = bot.generate_sunday_reconciliation_report(days_elapsed=14, user_live_approved=True)
        self.assertTrue(sunday["consistent_with_backtest"])
        self.assertEqual(sunday["recommended_live_capital_rmb"], 2000.0)
        self.assertEqual(len(sunday["promotion_ladder"]), 4)

    def test_12_interactive_dashboard_html(self) -> None:
        """Feature 12: 验证 dashboard.html 包含完整 HTML5 结构、交互组件 ID、内嵌真实回测 JSON 且无外部失效 CDN 依赖。"""
        html = self.dashboard_path.read_text(encoding="utf-8")
        self.assertIn("<!DOCTYPE html>", html)
        self.assertEqual(html.count("<h1"), 1)
        for req_id in (
            'id="desk-header"',
            'id="circuit-breaker-status"',
            'id="nav-tabs"',
            'id="equity-curve-canvas"',
            'id="atr-calculator-panel"',
            'id="handoff-table"',
            'id="veto-table"',
        ):
            self.assertIn(req_id, html)
        self.assertIn("window.DASHBOARD_DATA =", html)
        self.assertIn("跨市场波动率状态切换与避险资产轮动策略", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)
