import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, brier_score_loss, log_loss


def choose_model():
    try:
        from catboost import CatBoostClassifier  # type: ignore
        print("[OK] CatBoost H2H")
        return CatBoostClassifier(verbose=False, depth=6, iterations=200), "catboost"
    except Exception:
        print("[WARN] CatBoost missing; using LogisticRegression")
        return LogisticRegression(max_iter=1000), "scikit"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", default="data/featurized/h2h.csv")
    ap.add_argument("--train", action="store_true")
    ap.add_argument("--predict", action="store_true")
    ap.add_argument("--year", type=int)
    ap.add_argument("--race", type=int)
    ap.add_argument("--driver_a")
    ap.add_argument("--driver_b")
    args = ap.parse_args()

    model, fam = choose_model()
    Path("models_h2h").mkdir(exist_ok=True)
    h = pd.read_csv(args.infile)
    feats = [c for c in h.columns if c.startswith("diff_") or c in ["same_team_flag", "same_make_flag"]]
    h = h.dropna(subset=["target_a_beats_b"])
    split = int(len(h) * 0.8) if len(h) > 1 else 1
    tr = h.iloc[:split]
    te = h.iloc[split:] if split < len(h) else h.iloc[-1:]
    imp = SimpleImputer(strategy="median")
    Xtr = imp.fit_transform(tr[feats])
    Xte = imp.transform(te[feats]) if not te.empty else Xtr
    model.fit(Xtr, tr["target_a_beats_b"])
    p = model.predict_proba(Xte)[:, 1]
    if len(np.unique(te["target_a_beats_b"])) > 1:
        print(f"AUC={roc_auc_score(te['target_a_beats_b'], p):.3f} Brier={brier_score_loss(te['target_a_beats_b'], p):.3f} logloss={log_loss(te['target_a_beats_b'], p):.3f}")

    if args.train:
        import joblib

        joblib.dump(model, "models_h2h/h2h_model.pkl")
        Path("models_h2h/h2h_meta.json").write_text(json.dumps({"family": fam, "features": feats}, indent=2), encoding="utf-8")
        print("[OK] saved h2h model")

    if args.predict:
        d = pd.read_csv("data/featurized/data_featurized.csv")
        sub = d[(d["year"] == args.year) & (d["season_race_num"] == args.race)]
        if sub.empty:
            print("No race rows found. Run get_entries/build_dataset/featurize first.")
            return
        a = sub[sub["Driver"].str.lower() == args.driver_a.lower()]
        b = sub[sub["Driver"].str.lower() == args.driver_b.lower()]
        if a.empty or b.empty:
            print("[WARN] one or both drivers not found for race")
            return
        a = a.iloc[0]
        b = b.iloc[0]
        rec = {"same_team_flag": int(a.get("Team", "") == b.get("Team", "")), "same_make_flag": int(a.get("Make", "") == b.get("Make", ""))}
        for c in [x.replace("diff_", "") for x in feats if x.startswith("diff_")]:
            rec[f"diff_{c}"] = pd.to_numeric(a.get(c), errors="coerce") - pd.to_numeric(b.get(c), errors="coerce")
        X = imp.transform(pd.DataFrame([rec])[feats])
        pa = model.predict_proba(X)[:, 1][0]
        print(f"P({args.driver_a} ahead of {args.driver_b}) = {pa:.3f}")
        print(f"P({args.driver_b} ahead of {args.driver_a}) = {1-pa:.3f}")


if __name__ == "__main__":
    main()
