"""素材处理：视频上传（带确认按钮）"""
import logging
import tempfile
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from fb import FBError
from store.state import get_fb

logger = logging.getLogger(__name__)


async def upload_video_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    收到视频文件 → 显示确认按钮，用户确认后才上传到 FB
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
    file_id = file_obj.file_id
    file_name = getattr(file_obj, "file_name", None) or "video.mp4"
    file_size = getattr(file_obj, "file_size", 0)
    size_mb = f"{file_size / 1024 / 1024:.1f}MB" if file_size else "未知大小"

    # 来源信息
    sender = update.message.from_user
    sender_name = sender.first_name if sender else "未知"
    is_bot = sender.is_bot if sender else False
    bot_tag = " 🤖" if is_bot else ""

    # 保存 file_id 到 chat_data，等确认后再下载上传
    ctx.chat_data["pending_video_file_id"] = file_id
    ctx.chat_data["pending_video_file_name"] = file_name

    keyboard = [
        [InlineKeyboardButton("✅ 使用这个素材", callback_data="video_confirm:yes"),
         InlineKeyboardButton("❌ 不使用", callback_data="video_confirm:no")],
    ]
    await update.message.reply_text(
        f"🎬 收到视频素材\n\n"
        f"来源：{sender_name}{bot_tag}\n"
        f"文件：{file_name}（{size_mb}）\n\n"
        f"是否使用这个视频作为广告素材？",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def video_confirm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """确认/放弃视频素材的按钮回调"""
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action != "yes":
        # 清理暂存
        ctx.chat_data.pop("pending_video_file_id", None)
        ctx.chat_data.pop("pending_video_file_name", None)
        await query.edit_message_text("❌ 已放弃该视频，等待新的素材。")
        return

    # 检查是否有待确认的视频
    file_id = ctx.chat_data.get("pending_video_file_id")
    file_name = ctx.chat_data.get("pending_video_file_name", "video.mp4")
    if not file_id:
        await query.edit_message_text("❌ 没有待确认的视频。")
        return

    fb = get_fb(update.effective_chat.id)
    if not fb:
        await query.edit_message_text("❌ 未配置 FB 账户。")
        return

    await query.edit_message_text("⏳ 正在下载并上传视频到 FB...")

    # 下载并上传
    try:
        tg_file = await ctx.bot.get_file(file_id)
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        await tg_file.download_to_drive(tmp_path)
        video_id = fb.upload_video(tmp_path, title=file_name)

        ctx.chat_data["last_video_id"] = video_id
        # 清理暂存
        ctx.chat_data.pop("pending_video_file_id", None)
        ctx.chat_data.pop("pending_video_file_name", None)

        await query.edit_message_text(
            f"✅ 视频上传成功！\n\n"
            f"视频 ID：{video_id}\n"
            f"文件：{file_name}\n\n"
            f"用 /publish_last 绑定到广告组并发布",
        )
    except FBError as e:
        await query.edit_message_text(f"❌ 上传失败：{e}")
    except Exception as e:
        logger.error(f"视频上传异常: {e}")
        await query.edit_message_text(f"❌ 上传异常：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)
