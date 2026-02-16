param(
  [int]$StartYear = 2023,
  [int]$EndYear = 2024,
  [ValidateSet('prequal','postqual')][string]$Mode = 'prequal',
  [switch]$RunH2H
)

$ErrorActionPreference = 'Stop'
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
New-Item -ItemType Directory -Force -Path data/raw,data/enrich,data/featurized,data/dim,models,models_h2h,reports,cache/html | Out-Null
$log = "reports/pipeline_$ts.log"

function Run-Step($cmd) {
  Write-Host "[STATUS] $cmd"
  Add-Content -Path $log -Value "[STATUS] $cmd"
  iex $cmd 2>&1 | Tee-Object -FilePath $log -Append
  if ($LASTEXITCODE -ne 0) { Write-Host "[ERROR] failed: $cmd"; exit $LASTEXITCODE }
  Write-Host "[OK] $cmd"
}

Run-Step "python scripts/get_results.py --start_year $StartYear --end_year $EndYear"
Run-Step "python scripts/normalize_ids.py"
Run-Step "python scripts/enrich_track_meta.py"
Run-Step "python scripts/enrich_race_structure.py"
Run-Step "python scripts/enrich_weather.py"
Run-Step "python scripts/get_entries.py --year $EndYear --race 1"
Run-Step "python scripts/get_qualifying.py --year $EndYear --race 1"
Run-Step "python scripts/build_dataset.py"
Run-Step "python scripts/validate_data.py"
Run-Step "python scripts/featurizeData.py --mode $Mode"
Run-Step "python scripts/train_predict.py --train"
if ($RunH2H) {
  Run-Step "python scripts/build_h2h_dataset.py"
  Run-Step "python scripts/h2h_predict.py --train"
}
Write-Host "[OK] Pipeline complete. Log: $log"
