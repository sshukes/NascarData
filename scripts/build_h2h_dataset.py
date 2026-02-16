import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", default="data/featurized/data_featurized.csv")
    ap.add_argument("--out", default="data/featurized/h2h.csv")
    ap.add_argument("--pairs_per_race", type=int, default=50)
    ap.add_argument("--include_dnfs", action="store_true")
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.infile)
    num_cols = [c for c in df.columns if c.startswith("drv_")]
    rows = []
    rng = np.random.default_rng(42)
    for sked_id, grp in df.groupby("sked_id"):
        g = grp.copy()
        if not args.include_dnfs and "target_dnf" in g.columns:
            g = g[g["target_dnf"] != 1]
        if len(g) < 2:
            continue
        pairs = set()
        max_pairs = min(args.pairs_per_race, len(g) * (len(g) - 1) // 2)
        while len(pairs) < max_pairs:
            i, j = sorted(rng.choice(len(g), size=2, replace=False))
            pairs.add((i, j))
        g = g.reset_index(drop=True)
        for i, j in pairs:
            a, b = g.iloc[i], g.iloc[j]
            rec = {
                "sked_id": sked_id,
                "year": a["year"],
                "season_race_num": a["season_race_num"],
                "track_type": a.get("track_type", "unknown"),
                "track_id": a.get("track_id", ""),
                "race_date": a.get("race_date", ""),
                "driver_a_id": a.get("driver_id", ""),
                "driver_b_id": b.get("driver_id", ""),
                "DriverA": a.get("Driver", ""),
                "DriverB": b.get("Driver", ""),
                "target_a_beats_b": int(pd.notna(a.get("target_finish")) and pd.notna(b.get("target_finish")) and a.get("target_finish") < b.get("target_finish")),
                "target_finish_diff": (b.get("target_finish") - a.get("target_finish")) if pd.notna(a.get("target_finish")) and pd.notna(b.get("target_finish")) else np.nan,
                "same_team_flag": int(a.get("Team", "") == b.get("Team", "")),
                "same_make_flag": int(a.get("Make", "") == b.get("Make", "")),
            }
            for c in num_cols:
                rec[f"diff_{c}"] = pd.to_numeric(a.get(c), errors="coerce") - pd.to_numeric(b.get(c), errors="coerce")
            rows.append(rec)
    out = pd.DataFrame(rows)
    out.to_csv(args.out, index=False)
    print(f"[OK] wrote {args.out} rows={len(out)}")


if __name__ == "__main__":
    main()
