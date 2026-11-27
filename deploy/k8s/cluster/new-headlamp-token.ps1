$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$kubectl = Join-Path $repoRoot 'deploy\k8s\local\bin\kubectl.exe'
$kubeconfig = Join-Path $repoRoot 'deploy\k8s\local\admin.conf'

if (-not (Test-Path -LiteralPath $kubectl) -or -not (Test-Path -LiteralPath $kubeconfig)) {
    throw 'Local kubectl or private kubeconfig is missing.'
}

$token = & $kubectl --kubeconfig $kubeconfig --context airtraffic-kubeadm -n headlamp create token headlamp-login-admin --duration=8h
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Could not create a Headlamp login token.'
}

Set-Clipboard -Value $token
Remove-Variable token
Write-Host 'Headlamp login token copied to clipboard (valid for up to 8 hours).'
