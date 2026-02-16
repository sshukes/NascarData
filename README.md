# NASCAR Cup Series Local Prediction Pipeline

Local-only Python CLI pipeline for collecting NASCAR Cup data, building append-only datasets, featurizing without leakage, training baseline models, and generating race + H2H predictions.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Quickstart

```powershell
./run_pipeline.ps1 -StartYear 2023 -EndYear 2024 -Mode prequal
python scripts/train_predict.py --predict --year 2024 --race 5 --top 20
python scripts/h2h_predict.py --predict --year 2024 --race 5 --driver_a "Kyle Larson" --driver_b "Denny Hamlin"
```

## Script examples

- Collect results:
  - `python scripts/get_results.py --start_year 2023 --end_year 2024 --max_races 36`
- Collect entries:
  - `python scripts/get_entries.py --year 2024 --race 7`
- Collect qualifying:
  - `python scripts/get_qualifying.py --year 2024 --race 7`
- Normalize IDs:
  - `python scripts/normalize_ids.py`
- Enrich:
  - `python scripts/enrich_track_meta.py`
  - `python scripts/enrich_race_structure.py`
  - `python scripts/enrich_weather.py`
- Build + validate:
  - `python scripts/build_dataset.py`
  - `python scripts/validate_data.py`
- Featurize:
  - `python scripts/featurizeData.py --mode prequal`
  - `python scripts/featurizeData.py --mode postqual`
- Train/predict:
  - `python scripts/train_predict.py --train`
  - `python scripts/train_predict.py --predict --year 2024 --race 7 --top 20`
  - `python scripts/train_predict.py --compare_actual --year 2024 --race 7`
- H2H:
  - `python scripts/build_h2h_dataset.py --pairs_per_race 50`
  - `python scripts/h2h_predict.py --train`

## Troubleshooting

- 403/429 responses: scripts retry with backoff and print `[WARN]`; use `--sleep` and higher `--retries`.
- Offline mode: pass `--offline` and ensure cached HTML exists under `cache/html/...`.
- Cache bypass: use `--no_cache`.
- Optional deps (`catboost`, `pyarrow`, `duckdb`, `meteostat`) are not required; scripts continue with fallback and warn.

## Data contract highlights

- Master CSV: `data/raw/data.csv` (append-only + deterministic de-dupe).
- Key: `(sked_id, driver_id)` fallback `(sked_id, Driver, CarNumber)`.
- Stable sort: `race_date, year, season_race_num, driver_id`.
