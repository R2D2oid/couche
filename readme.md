# Couche-Tard Retail Analytics

Agentic AI chat interface for querying people-tracking data from the McGill Innovation Lab store (Nov 2025 – Mar 2026). Powered by Groq / LLaMA 3.3 70B with live data tools.

## Setup

### 1. Environment

**Local:**
```bash
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

**Compute Canada (Narval/Beluga):**
```bash
module load python/3.11.5 scipy-stack/2024a StdEnv/2023 arrow/17.0.0
source ENV/bin/activate
```

### 2. API Key

Copy `activate.sh`, fill in your Groq API key, then source it:
```bash
cp activate.sh activate.local.sh   # keep secrets out of git
# edit activate.local.sh and set GROQ_API_KEY
source activate.local.sh
```

Or export directly:
```bash
export GROQ_API_KEY="your-key-here"
```

Get a free key at [console.groq.com](https://console.groq.com).

## Running

### Web UI (recommended)

Local browser only:
```bash
python webui.py
# opens at http://127.0.0.1:7860
```

Public shareable link (Gradio tunnel, no account needed):
```bash
python webui.py --share
# prints a gradio.live URL valid for 72 hours
```

Custom port:
```bash
python webui.py --port 8080
python webui.py --share --port 8080
```

### Terminal chat

```bash
python main.py --chat
```

## On Compute Canada

```bash
cd /home/zahrav/projects/def-jjclark/zahrav/repos/couche
module load python/3.11.5 scipy-stack/2024a StdEnv/2023 arrow/17.0.0
source ENV/bin/activate
export GROQ_API_KEY="your-key-here"
python webui.py --share
```

The `--share` flag prints a public `gradio.live` URL you can open from any browser without SSH port forwarding.

## Project Structure

```
webui.py          Gradio web UI entry point
main.py           CLI entry point
src/
  chat.py         Agentic loop (Groq API + tool use)
  tools.py        Tool schemas and dispatch
  agents/         Specialized query agents (conversion, crowd, groups)
  db.py           DuckDB data access
  memory.py       ChromaDB conversation memory
  etl.py          Data pipeline
scratch/          Raw data files
chroma_cache/     ChromaDB persistent store
```
