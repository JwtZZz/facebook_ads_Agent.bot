"""主入口 — 同时启动 Telegram Bot + Dashboard Web 服务"""
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    from bot.app import build_app
    from services.web import create_web_app, start_web_server

    # 构建 Telegram Bot
    tg_app = build_app()

    # 构建并启动 Dashboard Web 服务
    web_app = create_web_app()
    port = int(os.getenv("DASHBOARD_PORT", "8080"))
    runner = await start_web_server(web_app, host="0.0.0.0", port=port)

    # 启动 Telegram Bot（手动生命周期管理）
    async with tg_app:
        await tg_app.start()
        await tg_app.updater.start_polling()
        logger.info("=== 主调度机器人 + Dashboard 已启动 ===")

        # 阻塞运行，直到进程被终止
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass

        await tg_app.updater.stop()
        await tg_app.stop()

    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
