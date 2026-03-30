"""素材处理：视频上传"""
import logging
import tempfile
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

from fb import FBError
from store.state import get_fb

logger = logging.getLogger(__name__)


async def upload_video_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    直接发送视频文件给 Bot，自动上传到 FB 广告账户
    """
    fb = get_fb(update.effective_chat.id)
    if not fb:
        await update.message.reply_text(
            "⚠️ 未配置 FB 账户，请先发送：\n"
            "`/setfb <token> <account_id> [pixel_id] [page_id]`",
            parse_mode="Markdown",
        )
        return

    if not update.message.video and not update.message.document:
        await update.message.reply_text(
            "请直接发送视频文件（mp4），我会上传到 FB 广告账户。"
        )
        return

    file_obj = update.message.video or update.message.document
    msg = await update.message.reply_text("⏳ 正在下载视频...")

    tg_file = await file_obj.get_file()
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        await tg_file.download_to_drive(tmp_path)
        await msg.edit_text("⏳ 视频下载完成，正在上传到 FB...")

        file_name = getattr(file_obj, "file_name", None) or "video.mp4"
        video_id  = fb.upload_video(tmp_path, title=file_name)

        ctx.chat_data["last_video_id"] = video_id
        await msg.edit_text(
            f"✅ 视频上传成功！\n"
            f"视频 ID: `{video_id}`\n\n"
            f"用 /publish_last 绑定到最近创建的广告组",
            parse_mode="Markdown",
        )
    except FBError as e:
        await msg.edit_text(f"❌ 上传失败: {e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)
