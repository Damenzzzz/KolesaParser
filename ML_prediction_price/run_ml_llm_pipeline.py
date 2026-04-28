from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .LLM.main import explain_ranked_cars
    from .ranker import rank_cars
    from .score_query_results import resolve_existing_path, resolve_output_path, score_parser_payload, write_json
except ImportError:
    from LLM.main import explain_ranked_cars
    from ranker import rank_cars
    from score_query_results import resolve_existing_path, resolve_output_path, score_parser_payload, write_json


def run_pipeline(query_path: str | Path, input_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    resolved_query = resolve_existing_path(query_path)
    resolved_input = resolve_existing_path(input_path)
    resolved_output = resolve_output_path(output_path)

    with resolved_query.open("r", encoding="utf-8") as file:
        query = json.load(file)
    with resolved_input.open("r", encoding="utf-8") as file:
        parser_payload = json.load(file)

    scored_payload = score_parser_payload(parser_payload, resolved_input)
    ranked = rank_cars(scored_payload["cars"], top_n=10)
    explanation = explain_ranked_cars(query, ranked)

    final_payload = {
        "query": query,
        "source_file": str(resolved_input),
        "total_cars": scored_payload["count"],
        "scored_cars": scored_payload["scored_count"],
        "failed_cars": scored_payload["failed_count"],
        "top_cars": ranked,
        "llm_summary": explanation["summary"],
        "llm": explanation,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(resolved_output, final_payload)
    final_payload["output_file"] = str(resolved_output)
    return final_payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ML scoring, ranking, and LLM/fallback explanation.")
    parser.add_argument("--query", required=True, help="Query JSON file used by KolesaParser.")
    parser.add_argument("--input", required=True, help="Parser output JSON file with cars.")
    parser.add_argument("--output", required=True, help="Final output JSON path.")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    args = build_arg_parser().parse_args()
    result = run_pipeline(args.query, args.input, args.output)
    print(f"Total cars: {result['total_cars']}")
    print(f"Scored cars: {result['scored_cars']}")
    print(f"Failed cars: {result['failed_cars']}")
    print(f"Output JSON: {result['output_file']}")


if __name__ == "__main__":
    main()
