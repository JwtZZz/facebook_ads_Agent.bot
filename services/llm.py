"""通义千问 LLM 对话服务（流式输出）"""
import asyncio
import os
import logging
from openai import AsyncOpenAI
from store.state import chat_histories

logger = logging.getLogger(__name__)

MODEL = os.getenv("LLM_MODEL", "qwen-turbo")

SYSTEM_PROMPT = (
    "你是一个 Facebook 广告投放主调度机器人，负责协助优化师管理广告账户。"
    "回答简洁专业，涉及 FB 广告操作时给出具体步骤。"
)

_llm: AsyncOpenAI | None = None


def _get_llm() -> AsyncOpenAI:
    global _llm
    if _llm is None:
        api_key = os.getenv("LLM_API_KEY")
        base_url = os.getenv("LLM_BASE_URL")
        if not api_key:
            raise RuntimeError("LLM_API_KEY 未设置")
        _llm = AsyncOpenAI(api_key=api_key, base_url=base_url)
    return _llm


async def ask_llm(chat_id: int, user_message: str, reply_message=None) -> str:
    """
    调用 LLM，如果传入 reply_message 则流式更新 Telegram 消息
    """
    if chat_id not in chat_histories:
        chat_histories[chat_id] = [{"role": "system", "content": SYSTEM_PROMPT}]

    history = chat_histories[chat_id]
    history.append({"role": "user", "content": user_message})

    if len(history) > 41:
        history[:] = [history[0]] + history[-40:]

    try:
        if reply_message:
            return await _stream_reply(history, reply_message)
        else:
            llm = _get_llm()
            resp = await llm.chat.completions.create(model=MODEL, messages=history)
            reply = resp.choices[0].message.content
            history.append({"role": "assistant", "content": reply})
            return reply
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        return f"调用大模型出错: {e}"


async def generate_ad_copy_variant(original_text: str, original_title: str) -> tuple[str, str]:
    """
    基于原始广告文案生成一个变体（单次调用，不写入 chat 历史）
    返回 (new_text, new_title)，失败时回退到原始文案
    """
    try:
        llm = _get_llm()
        prompt = (
            "你是一名 Facebook 广告文案专家。\n"
            "请根据下面的广告正文和标题，生成一个**内容相近但措辞不同**的变体，"
            "保持相同语言、相同风格和相同落地意图。\n"
            "只输出以下格式，不要任何其他内容：\n"
            "正文：<新正文>\n"
            "标题：<新标题>\n\n"
            f"原正文：{original_text or '（空）'}\n"
            f"原标题：{original_title or '（空）'}"
        )
        resp = await llm.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        output = resp.choices[0].message.content.strip()
        new_text = original_text
        new_title = original_title
        for line in output.splitlines():
            if line.startswith("正文："):
                new_text = line[3:].strip()
            elif line.startswith("标题："):
                new_title = line[3:].strip()
        return new_text, new_title
    except Exception as e:
        logger.error(f"generate_ad_copy_variant 失败: {e}")
        return original_text, original_title


async def _stream_reply(history: list, msg) -> str:
    """流式接收 LLM 回复，每隔一段时间更新 Telegram 消息"""
    llm = _get_llm()
    stream = await llm.chat.completions.create(
        model=MODEL, messages=history, stream=True,
    )

    full_text = ""
    last_edit = ""
    last_edit_time = 0

    async for chunk in stream:
        delta = chunk.choices[0].delta
        if delta.content:
            full_text += delta.content

        now = asyncio.get_event_loop().time()
        # 每 1 秒更新一次消息，避免触发 Telegram 频率限制
        if full_text != last_edit and (now - last_edit_time > 1.0):
            try:
                await msg.edit_text(full_text + " ▍")
                last_edit = full_text
                last_edit_time = now
            except Exception:
                pass

    # 最终更新（去掉光标）
    if full_text != last_edit:
        try:
            await msg.edit_text(full_text)
        except Exception:
            pass

    history.append({"role": "assistant", "content": full_text})
    return full_text
