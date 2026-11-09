#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一人对冲基金 — 量化视觉图表生成器 (中文原生排版)
==================================================
使用系统内置思源黑体 (Noto Sans CJK SC)，支持生成：
  1. 资产配置比例环形图 (Portfolio Allocation)
  2. 模拟盘净值与基准对比曲线图 (Equity Curve)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports"

# 统一配置高精度中文字体与暗色量化主题
plt.rcParams["font.sans-serif"] = ["Noto Sans CJK SC", "Noto Sans Mono CJK SC", "DejaVu Sans", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


def generate_portfolio_allocation_chart(
    items: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[Path] = None,
) -> Path:
    """生成资产配置环形图（全中文标注）"""
    save_path = output_path or (REPORTS_DIR / "portfolio_allocation_cn.png")
    save_path.parent.mkdir(parents=True, exist_ok=True)

    if not items:
        # 默认当前持仓组合
        items = [
            {"name": "易方达H股ETF (510900)", "amount": 4993.10, "pct": 25.5},
            {"name": "华夏恒生ETF (159920)", "amount": 4902.80, "pct": 25.0},
            {"name": "易方达医药ETF (512010)", "amount": 4991.10, "pct": 25.5},
            {"name": "中国石化 (600028)", "amount": 4640.00, "pct": 23.7},
            {"name": "现金及可用备付金", "amount": 464.95, "pct": 2.3},
        ]

    labels = [f"{item['name']}\n¥{item['amount']:,.0f} ({item['pct']:.1f}%)" for item in items]
    sizes = [item["amount"] for item in items]
    colors = ["#2962FF", "#00B0FF", "#00E676", "#FF9100", "#787B86"][:len(items)]
    explode = [0.03] * (len(items) - 1) + [0.01]

    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=160)
    fig.patch.set_facecolor("#161a25")
    ax.set_facecolor("#161a25")

    wedges, texts, autotexts = ax.pie(
        sizes,
        explode=explode,
        labels=labels,
        autopct="%1.1f%%",
        pctdistance=0.72,
        colors=colors,
        startangle=140,
        textprops=dict(color="#d1d4dc", fontsize=10, weight="bold"),
    )

    for autotext in autotexts:
        autotext.set_color("#ffffff")
        autotext.set_fontsize(9)
        autotext.set_weight("bold")

    ax.set_title("一人对冲基金 · ¥20,010 模拟盘资产配置结构", color="#ffffff", fontsize=14, weight="bold", pad=15)
    fig.text(0.5, 0.03, "风控铁律：单笔最大止损严格控制在 1,000 元以内 (5.0%) | 强制 2×ATR 动态保护", ha="center", color="#848e9c", fontsize=9)

    plt.tight_layout()
    plt.savefig(save_path, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close()

    return save_path


if __name__ == "__main__":
    p = generate_portfolio_allocation_chart()
    print(f"中文图表生成完毕: {p}")
