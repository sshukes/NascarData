import subprocess
from pathlib import Path


def test_smoke_pipeline_outputs(tmp_path):
    subprocess.run(
        [
            "python", "scripts/get_results.py", "--start_year", "2024", "--end_year", "2024", "--max_races", "2", "--offline",
            "--out", str(tmp_path / "data.csv"),
        ],
        check=True,
    )
    assert (tmp_path / "data.csv").exists()
