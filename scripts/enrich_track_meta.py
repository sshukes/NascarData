import argparse
from pathlib import Path
import pandas as pd


def classify(length):
    if pd.isna(length):
        return "intermediate_1p5"
    if length >= 2.0:
        return "superspeedway"
    if length < 1.0:
        return "short_flat"
    if abs(length - 1.0) < 0.05:
        return "flat_1mile"
    return "intermediate_1p5"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/enrich/track_meta.csv")
    args = ap.parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    tracks = pd.read_csv("data/dim/track_dim.csv") if Path("data/dim/track_dim.csv").exists() else pd.DataFrame(columns=["track_id", "track_canonical"])
    if tracks.empty and Path("data/raw/data.csv").exists():
        raw = pd.read_csv("data/raw/data.csv")
        tracks = raw[["track"]].drop_duplicates().reset_index(drop=True)
        tracks["track_id"] = [f"track_{i+1:04d}" for i in range(len(tracks))]
        tracks = tracks.rename(columns={"track": "track_canonical"})

    tracks["rr_track_url"] = ""
    tracks["track_length_mi"] = 1.5
    tracks["track_surface"] = "asphalt"
    tracks["banking_turns_deg"] = pd.NA
    tracks["banking_front_deg"] = pd.NA
    tracks["banking_back_deg"] = pd.NA
    tracks["layout_type"] = "oval"
    tracks["track_lat"] = 35.0
    tracks["track_lon"] = -80.0
    tracks["track_timezone"] = "America/New_York"
    tracks["track_type"] = tracks["track_length_mi"].map(classify)

    if out.exists():
        old = pd.read_csv(out)
        df = pd.concat([old, tracks], ignore_index=True, sort=False).drop_duplicates(["track_id"], keep="last")
    else:
        df = tracks
    df.to_csv(out, index=False)
    print(f"[OK] wrote {out}")


if __name__ == "__main__":
    main()
