param(
    [switch]$SkipHelm,
    [switch]$SkipRace
)

$ErrorActionPreference = "Stop"
$env:GOTOOLCHAIN = "go1.26.5"

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host "==> $Name"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw ("{0} failed with exit code {1}" -f $Name, $LASTEXITCODE)
    }
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

if (-not $SkipRace) {
    Invoke-Step "go test race" {
        go test -race ./...
    }
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
    # These vulnerabilities are reachable only through Pulsar's Avro schema
    # dependency. github.com/hamba/avro/v2 has no fixed release in the Go
    # vulnerability database as of 2026-08-12. Keep this allowlist narrow so
    # every other reachable vulnerability continues to fail the build.
    $allowedUnfixed = @(
        "GO-2026-5046",
        "GO-2026-5047",
        "GO-2026-5048"
    )

    $sarifText = (& $govulncheckPath -format sarif ./... | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "govulncheck failed to produce a SARIF report"
    }
    $sarif = $sarifText | ConvertFrom-Json
    $reachable = @($sarif.runs[0].results | Where-Object { $_.level -eq "error" })
    $blocked = @($reachable | Where-Object { $_.ruleId -notin $allowedUnfixed })
    $accepted = @($reachable | Where-Object { $_.ruleId -in $allowedUnfixed })

    foreach ($finding in $accepted) {
        Write-Warning ("Temporarily accepting unfixed upstream vulnerability {0}; see docs/security-exceptions.md" -f $finding.ruleId)
    }
    if ($blocked.Count -gt 0) {
        $blocked | ForEach-Object {
            Write-Host ("BLOCKED {0}: {1}" -f $_.ruleId, $_.message.text)
        }
        throw ("govulncheck found {0} non-allowlisted reachable vulnerability/vulnerabilities" -f $blocked.Count)
    }
}

if (-not $SkipHelm) {
    Invoke-Step "helm template" {
        if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
            throw "helm is required for chart rendering checks; rerun with -SkipHelm to skip locally"
        }
        helm template http-pulsar-router ./charts/http-pulsar-router --set-string config.server.auth.bearerToken=ci-placeholder > $null
    }
}
