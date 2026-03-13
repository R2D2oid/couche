"""
Terminal chat loop with agentic tool use via Groq.
The LLM decides which tools to call based on the question.
Run via: python main.py --chat
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from tools import TOOL_SCHEMAS, call_tool

GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"

SYSTEM_PROMPT = """You are a retail analytics assistant for Couche-Tard (a convenience store chain).
You have access to people-tracking data from the McGill Innovation Lab store covering Nov 2025 – Mar 2026.

You have tools to query:
- Visitor conversion rates (by zone, hour, day of week, trend)
- Crowd levels and door-shopper abandonment
- Group behaviour and partial buying patterns
- Custom SQL on the raw data

Always use the tools to get real numbers before answering.
Be concise and specific. Cite the numbers you find. Give actionable recommendations."""


def _groq_request(messages: list, use_tools: bool = True) -> dict:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise ValueError("GROQ_API_KEY not set. Run: source activate.sh")

    body = {
        "model": GROQ_MODEL,
        "messages": messages,
        "max_tokens": 2048,
        "temperature": 0.2,
    }
    if use_tools:
        body["tools"] = TOOL_SCHEMAS
        body["tool_choice"] = "auto"

    payload = json.dumps(body).encode()
    req = urllib.request.Request(
        GROQ_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "couche-analytics/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def _run_agent(user_question: str, history: list) -> str:
    """
    Agentic loop:
    1. Send question + tool schemas to Groq
    2. If Groq wants to call tools, run them and feed results back
    3. Repeat until Groq gives a final text answer
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history + [
        {"role": "user", "content": user_question}
    ]

    while True:
        response = _groq_request(messages)
        choice   = response["choices"][0]
        message  = choice["message"]

        # Groq gave a final text answer
        if choice["finish_reason"] == "stop" or not message.get("tool_calls"):
            return message.get("content", "")

        # Groq wants to call tools
        messages.append(message)  # add assistant message with tool_calls

        for tc in message["tool_calls"]:
            tool_name = tc["function"]["name"]
            try:
                arguments = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                arguments = {}

            print(f"  [calling {tool_name}({', '.join(f'{k}={v}' for k,v in arguments.items())})]")
            result = call_tool(tool_name, arguments)
            if len(result) > 3000:
                result = result[:3000] + "\n... [truncated]"

            messages.append({
                "role":         "tool",
                "tool_call_id": tc["id"],
                "content":      result,
            })


def run_chat():
    print("\n" + "="*60)
    print("  Couche-Tard Retail Analytics — Chat Interface")
    print("  Model: Groq / " + GROQ_MODEL)
    print("  Type 'quit' or Ctrl+C to exit")
    print("="*60)
    print("\nExample questions:")
    print("  - What is the overall conversion rate?")
    print("  - Which zones convert best?")
    print("  - Is crowd causing people to leave?")
    print("  - How does Tuesday compare to Friday?")
    print("  - Is conversion improving over time?\n")

    history = []

    while True:
        try:
            question = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye.")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Goodbye.")
            break

        print("Assistant: ", end="", flush=True)
        try:
            answer = _run_agent(question, history)
            print(answer)

            # Keep last 6 turns in history (3 Q&A pairs)
            history.append({"role": "user",      "content": question})
            history.append({"role": "assistant",  "content": answer})
            history = history[-12:]

        except Exception as e:
            print(f"[Error: {e}]")

        print()
