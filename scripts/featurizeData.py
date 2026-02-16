import argparse
from pathlib import Path
import pandas as pd


def build_features(df, mode="prequal"):
    if df.empty:
        return df
    df = df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.sort_values(["driver_id", "race_date", "season_race_num"])

    df["target_finish"] = pd.to_numeric(df.get("Finish"), errors="coerce")
    df["target_top10"] = (df["target_finish"] <= 10).astype("Int64")
    df["target_top5"] = (df["target_finish"] <= 5).astype("Int64")
    df["target_win"] = (df["target_finish"] == 1).astype("Int64")
    df["target_dnf"] = df.get("Status", "").astype(str).str.contains("DNF|Accident|Engine", case=False, na=False).astype("Int64")

    grp = df.groupby("driver_id", group_keys=False)
    shifted_finish = grp["target_finish"].shift(1)
    shifted_top10 = grp["target_top10"].shift(1)
    shifted_dnf = grp["target_dnf"].shift(1)

    for w in (5, 10, 20):
        df[f"drv_finish_mean_{w}"] = shifted_finish.groupby(df["driver_id"]).rolling(w, min_periods=1).mean().reset_index(level=0, drop=True)
        df[f"drv_finish_std_{w}"] = shifted_finish.groupby(df["driver_id"]).rolling(w, min_periods=2).std().reset_index(level=0, drop=True)
        df[f"drv_top10_rate_{w}"] = shifted_top10.groupby(df["driver_id"]).rolling(w, min_periods=1).mean().reset_index(level=0, drop=True)
        df[f"drv_dnf_rate_{w}"] = shifted_dnf.groupby(df["driver_id"]).rolling(w, min_periods=1).mean().reset_index(level=0, drop=True)

    if mode == "postqual":
        start_shift = grp["Start"].shift(1) if "Start" in df.columns else pd.Series(index=df.index, dtype=float)
        q_shift = grp["qual_speed"].shift(1) if "qual_speed" in df.columns else pd.Series(index=df.index, dtype=float)
        df["drv_start_mean_10"] = start_shift.groupby(df["driver_id"]).rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
        df["drv_qual_speed_mean_10"] = q_shift.groupby(df["driver_id"]).rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
        if "Start" in df.columns:
            df["start_bucket"] = pd.cut(pd.to_numeric(df["Start"], errors="coerce"), bins=[0, 5, 12, 24, 40], labels=["front", "upper_mid", "mid", "back"])

    df = df.sort_values(["race_date", "year", "season_race_num", "driver_id"])
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["prequal", "postqual"], default="prequal")
    ap.add_argument("--infile", default="data/raw/data.csv")
    ap.add_argument("--out", default="data/featurized/data_featurized.csv")
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.infile)
    out = build_features(df, args.mode)
    out.to_csv(args.out, index=False)
    print("[OK] rolling features use shift(1) before rolling")
    print(f"[OK] wrote {args.out}")


if __name__ == "__main__":
    main()
