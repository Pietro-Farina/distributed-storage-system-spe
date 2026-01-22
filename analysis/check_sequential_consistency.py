import json
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict
import re
from typing import List, Tuple, Optional


# --------- parsing helpers ---------

REQID_RE = re.compile(r"^(?P<client>\d+)-(?P<seq>\d+)$")

def parse_ts_iso_z(s: str) -> int:
    """
    Parse timestamps like: 2026-01-22T18:02:31.615350400Z
    Return integer nanoseconds since epoch (best-effort).
    Python datetime supports microseconds; we'll keep microsecond precision.
    """
    # trim to microseconds to avoid ValueError (615350400 -> 615350)
    if s.endswith("Z"):
        s2 = s[:-1]
    else:
        s2 = s

    # split fractional seconds
    if "." in s2:
        base, frac = s2.split(".", 1)
        frac = (frac + "000000")[:6]  # microseconds
        s2 = f"{base}.{frac}"
    dt = datetime.fromisoformat(s2).replace(tzinfo=timezone.utc)
    # microsecond precision
    return int(dt.timestamp() * 1_000_000_000)


def parse_reqid(reqid: str) -> Optional[Tuple[int, int]]:
    """
    Return (client_id, seq_no) if format matches '10-2', else None
    """
    m = REQID_RE.match(reqid)
    if not m:
        return None
    return int(m.group("client")), int(m.group("seq"))


@dataclass(frozen=True)
class LogEvent:
    ts_ns: int
    node_id: int
    req_id: str
    op: str
    key: int
    version: int
    phase: str
    success: bool


def load_jsonl(path: str) -> List[LogEvent]:
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            events.append(LogEvent(
                ts_ns=parse_ts_iso_z(obj["ts"]),
                node_id=int(obj["nodeId"]),
                req_id=str(obj["reqId"]),
                op=str(obj["operation"]),
                key=int(obj["key"]),
                version=int(obj["version"]),
                phase=str(obj["phase"]),
                success=bool(obj["success"]),
            ))
    return events


# --------- SC-ish checks (DEC-only) ---------

def check_read_returns_latest_committed(events: List[LogEvent]) -> List[str]:
    """
    Necessary condition for sequential consistency:
    - Consider DEC_*_SUCCESS as the "commit" / "return" points.
    - For each key, track committed UPDATE versions over time.
    - Each successful DEC_GET_SUCCESS must return the latest committed version <= its timestamp.
    """
    errors = []

    # Keep only successful DEC events relevant to R/W
    dec = [e for e in events if e.success and e.phase.startswith("DEC_") and e.op in ("UPDATE", "GET")]
    dec.sort(key=lambda e: (e.ts_ns, e.req_id, e.node_id))  # deterministic tie-break

    committed_versions = defaultdict(list)  # key -> list of (ts_ns, version, req_id)

    for e in dec:
        if e.op == "UPDATE" and e.phase == "DEC_UPDATE_SUCCESS":
            committed_versions[e.key].append((e.ts_ns, e.version, e.req_id))

        elif e.op == "GET" and e.phase == "DEC_GET_SUCCESS":
            hist = committed_versions.get(e.key, [])
            # latest committed update with ts <= get.ts
            eligible = [v for (ts, v, _) in hist if ts <= e.ts_ns]

            expected = 0 if not eligible else max(eligible)
            if e.version != expected:
                errors.append(
                    f"[READ-LATEST] GET {e.req_id} key={e.key} returned v={e.version} "
                    f"but latest committed before it is v={expected}"
                )

    return errors


def check_per_client_program_order(events: List[LogEvent]) -> List[str]:
    """
    If reqId encodes program order (client-seq), check that DEC events respect it:
    For each client, seq numbers must appear in nondecreasing order in DEC_*_SUCCESS events.
    This is not required by SC unless you want to enforce client-issued order explicitly.
    """
    errors = []

    dec = [e for e in events if e.success and e.phase.startswith("DEC_") and e.op in ("UPDATE", "GET")]
    dec.sort(key=lambda e: (e.ts_ns, e.req_id, e.node_id))

    last_seq = {}  # client_id -> last_seq_seen
    for e in dec:
        parsed = parse_reqid(e.req_id)
        if not parsed:
            continue
        client, seq = parsed
        prev = last_seq.get(client)
        if prev is not None and seq < prev:
            errors.append(
                f"[PROG-ORDER] client={client} saw reqId={e.req_id} after seq={prev} "
                f"(timestamp order violates client program order assumption)"
            )
        last_seq[client] = max(prev, seq) if prev is not None else seq

    return errors


def run_all_checks(path: str) -> int:
    events = load_jsonl(path)

    errors = []
    errors += check_read_returns_latest_committed(events)
    errors += check_per_client_program_order(events)

    if not errors:
        print("Checks passed (DEC-based read-latest + per-client order)")
        return 0

    print("Violations found:")
    for err in errors:
        print("  -", err)
    return 1


if __name__ == "__main__":
    # change this to your file name
    exit(run_all_checks("events.jsonl"))
