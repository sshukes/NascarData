import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/enrich/race_meta.csv")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv("data/raw/data.csv") if Path("data/raw/data.csv").exists() else pd.DataFrame()
    if raw.empty:
        print("[WARN] no raw data")
        pd.DataFrame(columns=["sked_id"]).to_csv(out, index=False)
        return
    g = raw.groupby("sked_id", as_index=False).agg(year=("year", "first"), season_race_num=("season_race_num", "first"))
    g["rr_race_url"] = ""
    g["laps_scheduled"] = 267
    g["distance_mi_scheduled"] = 400
    g["stage1_len"] = 80
    g["stage2_len"] = 85
    g["stage3_len"] = 102
    g["num_cars"] = raw.groupby("sked_id")["Driver"].nunique().values
    g["cautions"] = pd.NA
    g["caution_laps"] = pd.NA
    g["scheduled_start_time_local"] = "14:00"
    if out.exists():
        df = pd.concat([pd.read_csv(out), g], ignore_index=True).drop_duplicates(["sked_id"], keep="last")
    else:
        df = g
    df.to_csv(out, index=False)
    print(f"[OK] wrote {out}")


if __name__ == "__main__":
    main()
