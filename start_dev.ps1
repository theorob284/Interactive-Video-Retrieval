# Move to project root
Set-Location $PSScriptRoot

# Start Django in the background
$django = Start-Process uv `
    -ArgumentList "run python backend/manage.py runserver --noreload" `
    -WorkingDirectory $PSScriptRoot `
    -PassThru `
    -NoNewWindow

# Start React in the current terminal
Set-Location "$PSScriptRoot\frontend"

Write-Host "Django: http://127.0.0.1:8000"
Write-Host "React:  http://localhost:5173"

try {
    npm run dev
}
finally {
    # Stop Django when React exits
    Stop-Process -Id $django.Id -Force -ErrorAction SilentlyContinue
}