#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一人对冲基金 — 企业微信群机器人推送模块 (WeCom Webhook Notifier)
================================================================
支持文本 (Text)、Markdown 消息推送，涵盖：
  1. 机器人握手联通测试
  2. 每日 07:30 晨报自动推送
  3. 交易员限价与止损挂单成交通知
  4. 2ATR 止损与三大风控熔断预警
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "wecom_config.json"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [WeCom推送] %(message)s"
)

class WeComNotifier:
    """企业微信 Webhook 机器人客户端"""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or self._load_webhook_from_env_or_file()

    def _load_webhook_from_env_or_file(self) -> str:
        # 1. 优先读取环境变量
        env_url = os.environ.get("WECOM_WEBHOOK_URL", "").strip()
        if env_url:
            return env_url
        
        env_key = os.environ.get("WECOM_ROBOT_KEY", "").strip()
        if env_key:
            return f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={env_key}"

        # 2. 读取配置文件
        if DEFAULT_CONFIG_PATH.exists():
            try:
                with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    url = cfg.get("webhook_url", "").strip()
                    if url:
                        if not url.startswith("http"):
                            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={url}"
                        return url
            except Exception as e:
                logging.warning(f"读取 WeCom 配置文件失败: {e}")

        return ""

    def send_raw_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """向企业微信发送原始 JSON 数据包"""
        if not self.webhook_url:
            logging.error("❌ 尚未配置企业微信 Webhook 地址！请提供 Webhook URL 或 key。")
            return {"errcode": -1, "errmsg": "Webhook URL missing"}

        req = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp_data = resp.read().decode("utf-8")
                res = json.loads(resp_data)
                if res.get("errcode") == 0:
                    logging.info("✅ 企业微信消息推送成功！")
                else:
                    logging.error(f"❌ 企业微信推送返回错误: {res}")
                return res
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="ignore")
            logging.error(f"❌ HTTP 错误 {e.code}: {err_body}")
            return {"errcode": e.code, "errmsg": err_body}
        except Exception as e:
            logging.error(f"❌ 发送消息异常: {e}")
            return {"errcode": -2, "errmsg": str(e)}

    def send_text(self, content: str, mentioned_list: Optional[list] = None) -> Dict[str, Any]:
        """发送纯文本消息"""
        payload = {
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": mentioned_list or []
            }
        }
        return self.send_raw_payload(payload)

    def send_markdown(self, markdown_text: str) -> Dict[str, Any]:
        """发送企业微信 Markdown 格式富文本消息"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown_text
            }
        }
        return self.send_raw_payload(payload)

    def send_image(self, image_path: str | Path) -> Dict[str, Any]:
        """发送图片消息 (支持普通微信直显，大小不超过 2MB)"""
        path = Path(image_path)
        if not path.exists():
            logging.error(f"❌ 图片文件不存在: {path}")
            return {"errcode": -1, "errmsg": "File not found"}

        with open(path, "rb") as f:
            data = f.read()

        if len(data) > 2 * 1024 * 1024:
            logging.error(f"❌ 图片大小 ({len(data)} 字节) 超过企业微信 2MB 限制！")
            return {"errcode": -1, "errmsg": "Image size exceeds 2MB"}

        b64_str = base64.b64encode(data).decode("utf-8")
        md5_str = hashlib.md5(data).hexdigest()

        payload = {
            "msgtype": "image",
            "image": {
                "base64": b64_str,
                "md5": md5_str
            }
        }
        return self.send_raw_payload(payload)

    def send_handshake_test(self) -> Dict[str, Any]:
        """发送握手测试消息 (个人微信完全直显)"""
        text = (
            "🔔 一人对冲基金 · 交易台连接测试\n"
            "------------------------------\n"
            "• 通信状态：通信通道联通正常 (已适配个人微信)\n"
            "• 统筹管理：幕僚长 (Chief of Staff)\n"
            "• 初始本金：¥20,000.00 RMB\n"
            "• 单笔最大风险：¥1,000.00 RMB (5.0%)\n"
            "• 标的市场：中国大陆 A 股 & 港股核心 ETF\n"
            "------------------------------\n"
            "交易台所有 6 位专职 Agent 已就绪，每日 07:30 晨报与订单执行将实时以纯文本格式推送至本群！"
        )
        return self.send_text(text)

    def send_order_notification(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """发送限价单与止损单执行报告 (个人微信完全直显)"""
        text = (
            f"📈 【交易员执行指令】订单已下达\n"
            f"------------------------------\n"
            f"• 标的名称：{order.get('name')} ({order.get('symbol')})\n"
            f"• 交易市场：{order.get('market_label', '场内标的')}\n"
            f"• 委托方向：{order.get('side', 'BUY')} (限价成交)\n"
            f"• 成交价格：¥{order.get('limit_price', 0.0):.3f}\n"
            f"• 成交数量：{order.get('shares', 0)} 股 ({order.get('lots', 0)} 手)\n"
            f"• 占用资金：¥{order.get('notional_rmb', 0.0):.2f}\n"
            f"• 强制止损价：¥{order.get('stop_loss_price', 0.0):.3f}\n"
            f"• 单笔最大风险：¥{order.get('max_risk_rmb', 0.0):.2f} (<= 1000 元)\n"
            f"• 共振评分：{order.get('chart_score', 0.0)}/10，风控批准放行\n"
            f"------------------------------"
        )
        return self.send_text(text)

    def send_morning_brief(self, brief_text: str) -> Dict[str, Any]:
        """推送每日 07:30 晨报 (个人微信完全直显)"""
        text = (
            "🌅 一人对冲基金 · 07:30 每日晨报\n"
            "==============================\n"
            f"{brief_text}\n"
            "=============================="
        )
        return self.send_text(text)


def main():
    parser = argparse.ArgumentParser(description="企业微信群机器人推送与测试工具")
    parser.add_argument("--test", action="store_true", help="发送握手连接测试消息")
    parser.add_argument("--webhook", type=str, default="", help="企业微信 Webhook URL 或 key")
    parser.add_argument("--msg", type=str, default="", help="自定义发送的文本消息")
    args = parser.parse_args()

    # 如果 CLI 提供了 webhook，则写入配置
    if args.webhook:
        url = args.webhook
        if not url.startswith("http"):
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={url}"
        cfg = {"enabled": True, "webhook_url": url}
        with open(DEFAULT_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        logging.info(f"已更新 Webhook 配置至 {DEFAULT_CONFIG_PATH}")

    notifier = WeComNotifier(webhook_url=args.webhook or None)

    if args.test:
        print("\n🚀 正在发送企业微信测试消息...")
        res = notifier.send_handshake_test()
        print(f"发送结果: {res}")
    elif args.msg:
        res = notifier.send_text(args.msg)
        print(f"发送结果: {res}")
    else:
        print("未指定动作。请使用 --test 发送握手测试，或 --webhook <URL> 配置机器人地址。")


if __name__ == "__main__":
    main()
