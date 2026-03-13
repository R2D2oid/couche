"""
Couche-Tard Retail Analytics — main entry point

Usage:
  python main.py --etl              # process raw zips → parquet
  python main.py --report           # run all agents + generate report
  python main.py --all              # etl + report
  python main.py --ask "question"   # single question, no chat loop
  python main.py --chat             # interactive terminal chat (agentic, MCP-style)
  python main.py --etl --force      # re-process all zips (overwrite existing)
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Couche-Tard Retail Analytics Agent")
    parser.add_argument("--etl",    action="store_true", help="Run ETL (zip → parquet)")
    parser.add_argument("--force",  action="store_true", help="Force re-process all zips")
    parser.add_argument("--report", action="store_true", help="Run agents + generate report")
    parser.add_argument("--all",    action="store_true", help="ETL + report")
    parser.add_argument("--ask",    type=str,            help="Ask a single question about the data")
    parser.add_argument("--chat",   action="store_true", help="Interactive terminal chat (agentic tool use)")
    args = parser.parse_args()

    if not any([args.etl, args.report, args.all, args.ask, args.chat]):
        parser.print_help()
        return

    # ── ETL ──────────────────────────────────────────────────────────────────
    if args.etl or args.all:
        logger.info("Starting ETL …")
        from etl import run_etl
        run_etl(force=args.force)

    # ── Interactive chat (agentic, MCP-style tool use) ────────────────────────
    if args.chat:
        from chat import run_chat
        run_chat()
        return

    # ── Single question ───────────────────────────────────────────────────────
    if args.ask:
        from chat import _run_agent
        print("\nAssistant: ", end="", flush=True)
        answer = _run_agent(args.ask, history=[])
        print(answer + "\n")
        return

    # ── Full report ───────────────────────────────────────────────────────────
    if args.report or args.all:
        logger.info("Running Conversion Agent …")
        from agents.conversion import analyze_conversion
        conversion = analyze_conversion()

        logger.info("Running Crowd & Door-Shopper Agent …")
        from agents.crowd import analyze_crowd
        crowd = analyze_crowd()

        logger.info("Running Group Agent …")
        from agents.groups import analyze_groups
        groups = analyze_groups()

        logger.info("Generating executive report …")
        from report import generate_report
        report_text, report_path = generate_report(conversion, crowd, groups)
        print("\n" + "=" * 60)
        print(report_text)
        print("=" * 60)
        logger.info("Report saved → %s", report_path)


if __name__ == "__main__":
    main()
