import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/data.csv")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    results = pd.read_csv("data/raw/results.csv") if Path("data/raw/results.csv").exists() else pd.DataFrame()
    entries = pd.read_csv("data/raw/entries.csv") if Path("data/raw/entries.csv").exists() else pd.DataFrame()

    base = results.copy() if not results.empty else entries.copy()
    if base.empty:
        pd.DataFrame().to_csv(out, index=False)
        print("[WARN] empty dataset")
        return

    if not entries.empty:
        combo = pd.concat([base, entries], ignore_index=True, sort=False)
        if "driver_id" not in combo.columns and "Driver" in combo.columns:
            combo["driver_id"] = combo["Driver"].astype(str).str.lower().str.replace(" ", "_", regex=False)
        base = combo.drop_duplicates(["sked_id", "driver_id"], keep="last")

    if Path("data/raw/qualifying.csv").exists() and "driver_id" in base.columns:
        q = pd.read_csv("data/raw/qualifying.csv")
        q = q[[c for c in ["sked_id", "driver_id", "Start", "qual_speed", "pole_speed", "qual_round"] if c in q.columns]]
        base = base.drop(columns=["Start", "qual_speed", "pole_speed", "qual_round"], errors="ignore").merge(q, on=["sked_id", "driver_id"], how="left")

    for p in ["data/enrich/race_meta.csv", "data/enrich/weather.csv"]:
        if Path(p).exists() and "sked_id" in base.columns:
            e = pd.read_csv(p)
            if "sked_id" in e.columns:
                base = base.merge(e, on="sked_id", how="left", suffixes=("", "_enrich"))

    if Path("data/enrich/track_meta.csv").exists() and "track" in base.columns:
        t = pd.read_csv("data/enrich/track_meta.csv")
        if "track_canonical" in t.columns:
            base = base.merge(t, left_on="track", right_on="track_canonical", how="left", suffixes=("", "_track"))

    if "driver_id" not in base.columns and "Driver" in base.columns:
        base["driver_id"] = base["Driver"].astype(str).str.lower().str.replace(" ", "_", regex=False)

    car = base["CarNumber"].astype(str) if "CarNumber" in base.columns else ""
    base["_k2"] = base["Driver"].astype(str) + "|" + car
    base = base.drop_duplicates(["sked_id", "driver_id"], keep="last")
    base = base.drop_duplicates(["sked_id", "_k2"], keep="last").drop(columns=["_k2"])
    base = base.sort_values(["race_date", "year", "season_race_num", "driver_id"])
    base.to_csv(out, index=False)
    try:
        base.to_parquet("data/raw/data.parquet", index=False)
    except Exception:
        print("[WARN] pyarrow missing; parquet skipped")
    print(f"[OK] wrote {out}")


if __name__ == "__main__":
    main()
