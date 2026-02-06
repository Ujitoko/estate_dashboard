param(
  [string]$ProjectRoot = (Resolve-Path "$PSScriptRoot\.."),
  [string]$Python = "python"
)

Set-Location $ProjectRoot
& $Python "apps/scraper/suumo_scraper.py" --output-dir "data/processed"
