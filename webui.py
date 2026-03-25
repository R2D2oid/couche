"""
Couche-Tard Retail Analytics — Gradio Web UI

Run locally:
    python webui.py

Public URL (free, no account needed):
    python webui.py --share
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import gradio as gr
from chat import _run_agent


def chat_fn(message: str, history: list) -> str:
    # Keep only the last 6 exchanges to avoid context overflow
    history = history[-6:]
    agent_history = [{"role": m["role"], "content": m["content"]} for m in history]
    try:
        return _run_agent(message, agent_history)
    except Exception as e:
        err = str(e).lower()
        if "context" in err or "token" in err or "length" in err or "rate_limit" in err:
            return "⚠️ The conversation got too long. Starting fresh — please re-ask your question."
        return f"⚠️ Error: {e}"


demo = gr.ChatInterface(
    fn=chat_fn,
title="Couche-Tard Retail Analytics",
    description=(
        "Ask questions about visitor behaviour at the McGill Innovation Lab store "
        "(Nov 2025 – Mar 2026). Powered by Groq / LLaMA 3.3 70B with live data tools."
    ),
    examples=[
        "What is the overall conversion rate?",
        "Which zones convert best?",
        "Is crowd causing people to leave?",
        "How do groups behave compared to solo shoppers?",
        "What are the peak abandonment hours?",
        "Is conversion improving over time?",
    ],
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--share", action="store_true", help="Create a public gradio.live URL")
    parser.add_argument("--port", type=int, default=7860)
    args = parser.parse_args()

    demo.launch(share=args.share, server_port=args.port, show_api=False, analytics_enabled=False)
