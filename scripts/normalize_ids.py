import argparse
from pathlib import Path
import pandas as pd


def mk_dim(values, prefix, canonical_col):
    uniq = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip()})
    return pd.DataFrame({f"{prefix}_id": [f"{prefix}_{i+1:04d}" for i in range(len(uniq))], canonical_col: uniq})


def main():
    argparse.ArgumentParser().parse_args()
    dim_dir = Path("data/dim")
    dim_dir.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv("data/raw/data.csv") if Path("data/raw/data.csv").exists() else pd.DataFrame()
    entries = pd.read_csv("data/raw/entries.csv") if Path("data/raw/entries.csv").exists() else pd.DataFrame()
    src = pd.concat([raw, entries], ignore_index=True, sort=False)

    driver_dim = mk_dim(src.get("Driver", pd.Series(dtype=str)), "driver", "Driver_canonical")
    team_dim = mk_dim(src.get("Team", pd.Series(dtype=str)), "team", "Team_canonical")
    track_dim = mk_dim(src.get("track", pd.Series(dtype=str)), "track", "track_canonical")

    driver_dim.to_csv(dim_dir / "driver_dim.csv", index=False)
    team_dim.to_csv(dim_dir / "team_dim.csv", index=False)
    track_dim.to_csv(dim_dir / "track_dim.csv", index=False)

    if not src.empty:
        if "Driver" in src.columns and not driver_dim.empty:
            alias = src[["Driver"]].dropna().drop_duplicates().merge(driver_dim, left_on="Driver", right_on="Driver_canonical", how="left")
            alias[["Driver", "driver_id"]].rename(columns={"Driver": "alias_name"}).drop_duplicates().to_csv(dim_dir / "driver_alias.csv", index=False)
        else:
            pd.DataFrame(columns=["alias_name", "driver_id"]).to_csv(dim_dir / "driver_alias.csv", index=False)

        if "Team" in src.columns and not team_dim.empty:
            alias = src[["Team"]].dropna().drop_duplicates().merge(team_dim, left_on="Team", right_on="Team_canonical", how="left")
            alias[["Team", "team_id"]].rename(columns={"Team": "alias_name"}).drop_duplicates().to_csv(dim_dir / "team_alias.csv", index=False)
        else:
            pd.DataFrame(columns=["alias_name", "team_id"]).to_csv(dim_dir / "team_alias.csv", index=False)

        if "track" in src.columns and not track_dim.empty:
            alias = src[["track"]].dropna().drop_duplicates().merge(track_dim, left_on="track", right_on="track_canonical", how="left")
            alias[["track", "track_id"]].rename(columns={"track": "alias_name"}).drop_duplicates().to_csv(dim_dir / "track_alias.csv", index=False)
        else:
            pd.DataFrame(columns=["alias_name", "track_id"]).to_csv(dim_dir / "track_alias.csv", index=False)
    print("[OK] wrote dim tables")


if __name__ == "__main__":
    main()
