from pathlib import Path
from scripts.get_results import parse_driveraverages_html


def test_driveraverages_parse_fixture():
    html = Path("tests/fixtures/driveraverages_sample.html").read_text(encoding="utf-8")
    rows = parse_driveraverages_html(html, 202401, 2024, 1, "http://x")
    assert len(rows) == 2
    assert rows[0]["Driver"] == "Kyle Larson"
    assert int(rows[1]["Finish"]) == 1
