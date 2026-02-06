param(
  [string]$TaskName = "SUUMO_Okusawa3_Daily"
)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
  Write-Host "Removed task: $TaskName"
} else {
  Write-Host "Task not found: $TaskName"
}
