param(
    [switch]$SkipHelm
)

$ErrorActionPreference = "Stop"
$env:GOTOOLCHAIN = "go1.26.4"

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host "==> $Name"
    & $Command
}

Invoke-Step "go fmt check" {
    $files = gofmt -l ./cmd ./internal
    if ($files) {
        $files | ForEach-Object { Write-Host $_ }
        throw "gofmt check failed"
    }
}

Invoke-Step "go vet" {
    go vet ./...
}

Invoke-Step "go test" {
    go test ./...
}

Invoke-Step "go test race" {
    go test -race ./...
}

Invoke-Step "go build" {
    New-Item -ItemType Directory -Force -Path ./bin | Out-Null
    go build -trimpath -buildvcs=false -ldflags="-s -w" -o ./bin/sr-forwarder-security-check ./cmd/sr-forwarder
}

Invoke-Step "govulncheck" {
    $govulncheck = Get-Command govulncheck -ErrorAction SilentlyContinue
    if (-not $govulncheck) {
        go install golang.org/x/vuln/cmd/govulncheck@latest
        $goPath = go env GOPATH
        $exe = "govulncheck"
        if ($IsWindows) {
            $exe = "govulncheck.exe"
        }
        $govulncheckPath = Join-Path $goPath (Join-Path "bin" $exe)
    } else {
        $govulncheckPath = $govulncheck.Source
    }
    & $govulncheckPath ./...
}

if (-not $SkipHelm) {
    Invoke-Step "helm template" {
        if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
            throw "helm is required for chart rendering checks; rerun with -SkipHelm to skip locally"
        }
        helm template http-pulsar-router ./charts/http-pulsar-router --set-string config.server.auth.bearerToken=ci-placeholder > $null
    }
}
