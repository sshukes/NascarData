import pandas as pd
from scripts.featurizeData import build_features


def test_rolling_is_shifted():
    df = pd.DataFrame(
        {
            "driver_id": ["a", "a", "a"],
            "Driver": ["A", "A", "A"],
            "year": [2024, 2024, 2024],
            "season_race_num": [1, 2, 3],
            "race_date": ["2024-01-01", "2024-01-08", "2024-01-15"],
            "Finish": [10, 20, 30],
            "Status": ["Running", "Running", "Running"],
        }
    )
    out = build_features(df, "prequal")
    row2 = out[out["season_race_num"] == 2].iloc[0]
    assert abs(row2["drv_finish_mean_5"] - 10.0) < 1e-9
