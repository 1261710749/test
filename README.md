# 一人对冲基金 (One-Person Quantitative Hedge Fund — 中国大陆A股与港股 20,000 RMB 专版)

本项目完整实现您的专属量化对冲基金交易台（*「新闻可以是全球，我只布局中国大陆和港股市场，可以是长回报周期，资金规模2w人民币」*），涵盖 6-Agent 协同工作流、2019–2026 真实前复权日线与三季报回测引擎、Top 3 获批策略（含 `#1 跨市场波动率状态切换与避险资产轮动策略`，样本外净收益 `+72.87%`，评分 `23/25`）、双模自动化交易机器人 (`strategy_rank1_bot.py`)、TradingView Pine v5 指标 (`pine/strategy_rank1_indicator.pine`)、交互式仪表盘 (`dashboard.html`) 及 12 项自动化单元测试套件 (`tests/test_suite.py`)。

## 一键运行与验证命令

```bash
# 1. 运行全周期真实数据拉取、6大策略回测、6-Agent日志与全部报告/仪表盘生成
python3 /home/antigravity/1261710749/test/quant_engine.py

# 2. 运行排名第1策略机器人的静态AST与运行时对抗安全审计
python3 /home/antigravity/1261710749/test/strategy_rank1_bot.py --mode audit

# 3. 运行覆盖全部 12 项核心功能的自动化单元测试套件
python3 -m unittest discover -s /home/antigravity/1261710749/test/tests -p "test_*.py" -v
```
