# Pakuje src/ (+ zależności z requirements.txt) do build/lambda.zip pod AWS Lambda (Python 3.12, Linux x86_64).
# Uruchamiaj z głównego katalogu repo w PowerShellu: .\scripts\build_lambda.ps1
$ErrorActionPreference = "Stop"

$Python = if (Test-Path ".venv\Scripts\python.exe") { ".\.venv\Scripts\python.exe" } else { "python" }

Remove-Item -Recurse -Force build -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path build\package | Out-Null

# Zależności tylko jeśli są (boto3 jest już w runtime Lambdy, nie dodawaj go)
if ((Test-Path "requirements.txt") -and ((Get-Item "requirements.txt").Length -gt 0)) {
    & $Python -m pip install -r requirements.txt -t build\package `
        --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 --implementation cp
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Copy-Item -Recurse -Force src\* build\package\
Get-ChildItem -Path build\package -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force

# Pakowanie przez Pythona gwarantuje ukośniki '/' wymagane przez linuksowy runtime AWS Lambda
& $Python -c "import os, zipfile; zf = zipfile.ZipFile('build/lambda.zip', 'w', zipfile.ZIP_DEFLATED); [zf.write(os.path.join(r, f), os.path.relpath(os.path.join(r, f), 'build/package').replace(os.sep, '/')) for r, _, fs in os.walk('build/package') for f in fs]; zf.close()"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$SizeMB = [math]::Round((Get-Item "build\lambda.zip").Length / 1KB, 1)
Write-Host "OK: build/lambda.zip ($SizeMB KB)"
