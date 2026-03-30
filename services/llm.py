"""通义千问 LLM 对话服务"""
import os
import logging
from openai import AsyncOpenAI
from store.state import chat_histories

logger = logging.getLogger(__name__)

_llm = AsyncOpenAI(
    api_key=os.getenv("LLM_API_KEY"),
    base_url=os.getenv("LLM_BASE_URL"),
)
MODEL = os.getenv("LLM_MODEL", "qwen-turbo")

SYSTEM_PROMPT = (
    "你是一个 Facebook 广告投放主调度机器人，负责协助优化师管理广告账户。"
    "回答简洁专业，涉及 FB 广告操作时给出具体步骤。"
)


async def ask_llm(chat_id: int, user_message: str) -> str:
    if chat_id not in chat_histories:
        chat_histories[chat_id] = [{"role": "system", "content": SYSTEM_PROMPT}]

    history = chat_histories[chat_id]
    history.append({"role": "user", "content": user_message})

    if len(history) > 41:
        history[:] = [history[0]] + history[-40:]

    try:
        resp = await _llm.chat.completions.create(model=MODEL, messages=history)
        reply = resp.choices[0].message.content
        history.append({"role": "assistant", "content": reply})
        return reply
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        return f"调用大模型出错: {e}"
