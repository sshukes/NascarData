import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/enrich/weather.csv")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    race = pd.read_csv("data/enrich/race_meta.csv") if Path("data/enrich/race_meta.csv").exists() else pd.DataFrame(columns=["sked_id"])
    if race.empty:
        pd.DataFrame(columns=["sked_id"]).to_csv(out, index=False)
        print("[WARN] no race meta")
        return
    wx = race[["sked_id"]].copy()
    wx["wx_temp_f"] = 72
    wx["wx_wind_mph"] = 8
    wx["wx_gust_mph"] = 14
    wx["wx_precip_in"] = 0.0
    wx["wx_precip_flag"] = 0
    wx["wx_time_used"] = "14:00"
    wx["wx_source"] = "fallback"
    if out.exists():
        df = pd.concat([pd.read_csv(out), wx], ignore_index=True).drop_duplicates(["sked_id"], keep="last")
    else:
        df = wx
    df.to_csv(out, index=False)
    print(f"[OK] wrote {out}")


if __name__ == "__main__":
    main()
