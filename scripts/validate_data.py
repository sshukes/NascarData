import json
from pathlib import Path
import pandas as pd


REQ = ["sked_id", "driver_id", "year", "season_race_num", "race_date", "race_name_raw", "race_name"]


def main():
    Path("reports").mkdir(exist_ok=True)
    p = Path("data/raw/data.csv")
    if not p.exists():
        raise SystemExit("[ERROR] missing data/raw/data.csv")
    df = pd.read_csv(p)
    issues = []
    critical = False

    miss = [c for c in REQ if c not in df.columns]
    if miss:
        issues.append(f"missing critical columns: {miss}")
        critical = True

    if all(c in df.columns for c in ["sked_id", "driver_id"]):
        dup = int(df.duplicated(["sked_id", "driver_id"]).sum())
        issues.append(f"duplicate_keys={dup}")
        if dup > 0:
            critical = True

    if "race_date" in df.columns:
        parsed = pd.to_datetime(df["race_date"], errors="coerce")
        issues.append(f"bad_dates={int(parsed.isna().sum())}")

    miss_pct = df.isna().mean().sort_values(ascending=False).head(10)
    issues.append("top_missing_pct=" + ", ".join([f"{k}:{v:.2f}" for k, v in miss_pct.items()]))

    leak_cols = [c for c in df.columns if "drv_finish_mean" in c]
    if leak_cols:
        issues.append("leakage_check=rolling_features_present_shift_required")

    Path("reports/data_quality_report.txt").write_text("\n".join(issues), encoding="utf-8")
    Path("reports/schema.json").write_text(json.dumps({"required_columns": REQ}, indent=2), encoding="utf-8")
    print("[OK] wrote reports/data_quality_report.txt")
    if critical:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
