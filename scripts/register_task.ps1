param(
  [string]$TaskName = "SUUMO_Okusawa3_Daily",
  [string]$Time = "06:30",
  [string]$ProjectRoot = (Resolve-Path "$PSScriptRoot\..")
)

$scriptPath = Join-Path $ProjectRoot "scripts\run_daily.ps1"
$pwsh = (Get-Command powershell.exe).Source

$action = New-ScheduledTaskAction -Execute $pwsh -Argument "-NoProfile -ExecutionPolicy Bypass -File \"$scriptPath\""
$trigger = New-ScheduledTaskTrigger -Daily -At $Time
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Write-Host "Registered task: $TaskName at $Time"
