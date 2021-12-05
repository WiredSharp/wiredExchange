& (Join-Path $PSScriptRoot 'activate.ps1')
$wired_exchangePath = $PSScriptRoot
Push-Location (Join-Path $PSScriptRoot 'streamlit')
try {
    if (-not $env:PYTHONPATH) {
        $env:PYTHONPATH = "$wired_exchangePath"
    } elseif ($env:PYTHONPATH -notmatch [regex]::escape($wired_exchangePath)) {
        $env:PYTHONPATH = "${env:PYTHONPATH};$wired_exchangePath"
    }
    & streamlit run .\wallet.py
} finally {
    Pop-Location
}