from pathlib import Path
import pytest

from src.transform import read_events, to_events, EXPECTED_COLUMNS

def test_schema_matches_expected(tmp_path: Path):
    p = tmp_path / "x.csv"
    p.write_text(
        ",".join(EXPECTED_COLUMNS) + "\n1,2,2026-02-01T00:00:00Z,purchase,1.0\n",
        encoding="utf-8",
    )
    rows = read_events(p)
    assert len(rows) == 1

def test_missing_user_id_fails(tmp_path: Path):
    p = tmp_path / "x.csv"
    p.write_text(
        ",".join(EXPECTED_COLUMNS) + "\n1,,2026-02-01T00:00:00Z,purchase,1.0\n",
        encoding="utf-8",
    )
    rows = read_events(p)
    with pytest.raises(ValueError):
        to_events(rows)
