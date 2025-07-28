import json, os, uuid, datetime, pathlib

LOG_DIR = pathlib.Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def new_run_id() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]

def write_audit(run_id: str, stage: str, payload: dict | str):
    """
    Append a JSONL line to logs/<run_id>.jsonl
    stage: "prompt" | "raw_response" | "parsed_intent" | "final_intent"
    payload: dict or raw string (converted to dict)
    """
    if isinstance(payload, str):
        payload = {"text": payload}

    line = {"stage": stage, "data": payload}
    log_file = LOG_DIR / f"{run_id}.jsonl"
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, default=str) + "\n")
