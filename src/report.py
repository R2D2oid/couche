"""
Report Generator — uses Groq API (free tier) with Llama 3.3 70B.
Requires GROQ_API_KEY (get free key at https://console.groq.com).
"""

import json
import os
import urllib.request
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
MAX_TOKENS = 4096


def _truncate(obj, max_items: int = 20):
    if isinstance(obj, list):
        return obj[:max_items]
    if isinstance(obj, dict):
        return {k: _truncate(v, max_items) for k, v in obj.items()}
    return obj


def _call_groq(prompt: str, max_tokens: int) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise ValueError("GROQ_API_KEY is not set. Get a free key at https://console.groq.com")

    payload = json.dumps({
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "couche-analytics/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    return result["choices"][0]["message"]["content"]


def _build_prompt(conversion: dict, crowd: dict, groups: dict, date_range: str | None) -> str:
    return f"""You are a senior retail analytics advisor for Couche-Tard convenience stores.

You have been given three sets of statistical results from people-tracking cameras
installed at the McGill Innovation Lab store{' for ' + date_range if date_range else ''}.

Your task is to write a concise **executive report** (≈600 words) for store managers.
Structure it as:

## Executive Summary
(3–4 bullet points – the most important findings)

## 1. Visitor Conversion
(Key numbers, best/worst zones and hours, trend direction)

## 2. Crowd & Door-Shopper Abandonment
(Is crowd causing walk-outs? At what occupancy? Peak times?)

## 3. Group Behaviour
(How often do groups partially buy? Where do non-buying group members go?)

## Recommendations (prioritised)
1. …
2. …
3. …

## Suggested Next Steps
(Data gaps, experiments to run, metrics to track)

---
### DATA

**CONVERSION ANALYSIS**
{json.dumps(_truncate(conversion), indent=2, default=str)}

**CROWD & DOOR-SHOPPER ANALYSIS**
{json.dumps(_truncate(crowd), indent=2, default=str)}

**GROUP BEHAVIOUR ANALYSIS**
{json.dumps(_truncate(groups), indent=2, default=str)}
"""


def generate_report(
    conversion: dict,
    crowd: dict,
    groups: dict,
    date_range: str | None = None,
) -> tuple[str, Path]:
    """Generate a markdown executive report and save it to reports/."""
    print(f"  Using Groq ({GROQ_MODEL})")
    report_text = _call_groq(_build_prompt(conversion, crowd, groups, date_range), MAX_TOKENS)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    report_path = REPORTS_DIR / f"report_{timestamp}.md"
    report_path.write_text(report_text, encoding="utf-8")

    return report_text, report_path


def ask(question: str, conversion: dict, crowd: dict, groups: dict) -> str:
    """Answer a natural-language question about the analysed data."""
    context = f"""You are a retail analytics expert for Couche-Tard.
You have the following pre-computed analysis results for the McGill Innovation Lab store.
Answer the question using specific numbers from the data.

CONVERSION: {json.dumps(_truncate(conversion), default=str)}
CROWD:      {json.dumps(_truncate(crowd),      default=str)}
GROUPS:     {json.dumps(_truncate(groups),     default=str)}

Question: {question}"""

    return _call_groq(context, max_tokens=1024)
