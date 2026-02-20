from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Event:
    event_id: int
    user_id: int
    event_ts: str
    event_type: str
    amount: float


EXPECTED_COLUMNS = ["event_id", "user_id", "event_ts", "event_type", "amount"]


def read_events(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(f"Schema mismatch. Got {reader.fieldnames}, expected {EXPECTED_COLUMNS}")
        return list(reader)


def to_events(rows: list[dict]) -> list[Event]:
    out: list[Event] = []
    for r in rows:
        if r["user_id"] in (None, ""):
            raise ValueError("user_id is required (found empty).")

        out.append(
            Event(
                event_id=int(r["event_id"]),
                user_id=int(r["user_id"]),
                event_ts=r["event_ts"],
                event_type=r["event_type"],
                amount=float(r["amount"]),
            )
        )
    return out


def write_cleaned(events: list[Event], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_COLUMNS)
        for e in events:
            writer.writerow([e.event_id, e.user_id, e.event_ts, e.event_type, f"{e.amount:.2f}"])


def main() -> None:
    in_path = Path("data/raw/events.csv")
    out_path = Path("data/processed/events_clean.csv")

    rows = read_events(in_path)
    events = to_events(rows)
    write_cleaned(events, out_path)
    print(f"Wrote {len(events)} cleaned events to {out_path}")


if __name__ == "__main__":
    main()
