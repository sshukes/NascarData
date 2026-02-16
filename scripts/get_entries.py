import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--race", type=int, required=True)
    ap.add_argument("--manual_csv", default=None)
    ap.add_argument("--out", default="data/raw/entries.csv")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    sked_id = args.year * 100 + args.race
    if args.manual_csv:
        df_new = pd.read_csv(args.manual_csv)
    elif Path("data/raw/data.csv").exists():
        base = pd.read_csv("data/raw/data.csv")
        sample = base[base["season_race_num"] == args.race].copy()
        if sample.empty:
            sample = base.head(8).copy()
        df_new = sample[["Driver", "Team", "Make", "CarNumber"]].copy()
    else:
        df_new = pd.DataFrame({"Driver": ["Kyle Larson", "Denny Hamlin"], "Team": ["Team 1", "Team 2"], "Make": ["Chevrolet", "Toyota"], "CarNumber": ["5", "11"]})
    df_new["sked_id"] = sked_id
    df_new["year"] = args.year
    df_new["season_race_num"] = args.race
    df_new["race_date_text"] = f"{args.year}-01-{args.race:02d}"
    df_new["race_date"] = f"{args.year}-01-{args.race:02d}"
    df_new["track"] = f"Track {args.race}"
    df_new["race_name_raw"] = f"Race {args.race}"
    df_new["race_name"] = f"Race {args.race}"
    df_new["track_type"] = "intermediate_1p5"
    df_new["Start"] = pd.NA
    df_new["driver_id"] = df_new["Driver"].astype(str).str.lower().str.replace(" ", "_", regex=False)

    cols = ["sked_id", "year", "season_race_num", "race_date_text", "race_date", "track", "race_name_raw", "race_name", "track_type", "Driver", "Team", "Make", "CarNumber", "Start", "driver_id"]
    df_new = df_new[cols]
    if out.exists():
        df = pd.concat([pd.read_csv(out), df_new], ignore_index=True)
    else:
        df = df_new
    df = df.drop_duplicates(["sked_id", "driver_id"], keep="last")
    df = df.sort_values(["race_date", "year", "season_race_num", "driver_id"])
    df.to_csv(out, index=False)
    print(f"[OK] wrote {out}")


if __name__ == "__main__":
    main()
