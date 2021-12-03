$venv = Join-Path $PSScriptRoot 'venv'
if (-not (Test-Path $venv)) {
   python -m venv $venv
   $requirements = (Join-Path $PSScriptRoot 'requirements.txt')
   if (Test-Path $requirements) {
      pip -r $requirements
   }
}

& ([System.IO.Path]::Combine($venv, 'scripts', 'activate.ps1') )