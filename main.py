"""主入口 — 加载环境变量并启动 Bot"""
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from bot.app import build_app

if __name__ == "__main__":
    app = build_app()
    logging.getLogger(__name__).info("=== 主调度机器人已启动 ===")
    app.run_polling()
