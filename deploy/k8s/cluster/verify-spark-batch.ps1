#requires -Version 7.0
param(
    [string]$Kubeconfig,
    [string]$Context = 'airtraffic-kubeadm'
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$kubectl = Join-Path $repoRoot 'deploy\k8s\local\bin\kubectl.exe'
if (-not $Kubeconfig) { $Kubeconfig = Join-Path $repoRoot 'deploy\k8s\local\admin.conf' }
$runId = 'batch-check-' + [DateTime]::UtcNow.ToString('yyyyMMddHHmmss')
$evidence = Join-Path $repoRoot "deploy\k8s\local\$runId"
New-Item -ItemType Directory -Path $evidence | Out-Null
$lakeRoot = "s3a://airtraffic-states/acceptance/$runId"

function Invoke-Kube {
    $result = & $kubectl --kubeconfig $Kubeconfig --context $Context --request-timeout=15s -n airtraffic @args
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed: $($args -join ' ')" }
    return $result
}

$cron = Invoke-Kube get cronjob airtraffic-recent-analytics -o json | ConvertFrom-Json -AsHashtable
if (-not $cron.spec.suspend) { throw 'Suspend the analytics CronJob before acceptance to prevent overlapping Spark batch workloads.' }
if ($cron.status.active.Count -gt 0) { throw 'Wait for the existing scheduled batch Job to finish.' }
$existingPods = Invoke-Kube get pods -o json | ConvertFrom-Json
$runningBatch = @($existingPods.items | Where-Object {
    $_.status.phase -in @('Pending', 'Running') -and 'analytics' -in $_.spec.containers.name
})
if ($runningBatch.Count -gt 0) { throw 'Another analytics driver is still active; finish it before acceptance.' }
if ($cron.spec.jobTemplate.spec.template.spec.serviceAccountName -ne 'spark-batch-driver') {
    throw 'Apply the batch-distributed overlay before acceptance.'
}
$cm = Invoke-Kube create configmap $runId "--from-file=acceptance.py=$(Join-Path $repoRoot 'deploy/k8s/checks/spark-batch/acceptance.py')" --dry-run=client -o json
$cmPath = Join-Path $evidence 'configmap.json'
$cm | Set-Content -LiteralPath $cmPath -Encoding utf8
Invoke-Kube apply -f $cmPath
Write-Host "Evidence: $evidence"
Write-Host "Frozen input and outputs: $lakeRoot"

foreach ($phase in @('snapshot', 'local', 'distributed', 'compare')) {
    $jobName = "$runId-$phase"
    $spec = $cron.spec.jobTemplate.spec | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
    $spec.activeDeadlineSeconds = 900
    $spec.template.metadata = @{ labels = @{ app = 'airtraffic-batch-acceptance'; 'acceptance-run' = $runId } }
    $pod = $spec.template.spec
    $container = $pod.containers[0]
    $container.volumeMounts += @{ name = 'acceptance'; mountPath = '/opt/acceptance'; readOnly = $true }
    $pod.volumes += @{ name = 'acceptance'; configMap = @{ name = $runId } }
    if ($phase -eq 'distributed') {
        $submit = @('--properties-file', '/etc/spark-batch/spark.properties',
            '--conf', 'spark.driver.host=$(POD_IP)', '--conf', 'spark.kubernetes.driver.pod.name=$(POD_NAME)')
    } else {
        $pod.serviceAccountName = 'default'
        $pod.automountServiceAccountToken = $false
        $submit = @('--master', 'local[2]', '--driver-memory', '768m', '--conf', 'spark.jars.ivy=/tmp/ivy')
    }
    if ($phase -in @('local', 'distributed')) {
        $submit += @('--conf', 'spark.eventLog.enabled=true', '--conf', "spark.eventLog.dir=$lakeRoot/events",
            '--conf', 'spark.eventLog.compress=false', '--conf', 'spark.eventLog.rolling.enabled=false')
    }
    $container.args = $submit + @('/opt/acceptance/acceptance.py', $phase, '--root', $lakeRoot)
    $job = @{ apiVersion = 'batch/v1'; kind = 'Job'; metadata = @{ name = $jobName; namespace = 'airtraffic' }; spec = $spec }
    $jobPath = Join-Path $evidence "$phase-job.json"
    $job | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $jobPath -Encoding utf8
    Invoke-Kube create -f $jobPath
    $deadline = [DateTime]::UtcNow.AddMinutes(16)
    while ($true) {
        $state = Invoke-Kube get job $jobName -o json | ConvertFrom-Json
        if ($phase -eq 'distributed') {
            $pods = Invoke-Kube get pods -l app=airtraffic-batch-executor -o json
            $observed = $pods | ConvertFrom-Json
            if ($observed.items.Count -gt 0) {
                $stamp = [DateTime]::UtcNow.ToString('HHmmss')
                $pods | Set-Content -LiteralPath (Join-Path $evidence "executors-$stamp.json") -Encoding utf8
            }
        }
        $finished = @($state.status.conditions | Where-Object { $_.type -in @('Complete', 'Failed') -and $_.status -eq 'True' })
        if ($finished.Count -gt 0) {
            $logs = Invoke-Kube logs "job/$jobName"
            $logs | Set-Content -LiteralPath (Join-Path $evidence "$phase.log") -Encoding utf8
            $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $evidence "$phase-result.json") -Encoding utf8
            if ($finished[0].type -eq 'Failed') { throw "$phase failed. Read $evidence/$phase.log; schedule remains suspended." }
            if (($logs -join "`n") -match '(?m)^\d{2}/\d{2}/\d{2} .* ERROR |^Traceback') {
                throw "$phase exited with errors in its log. Read $evidence/$phase.log before promotion."
            }
            Write-Host "$phase completed"
            if ($phase -eq 'compare') { $logs | Select-String 'BATCH_DISTRIBUTED_ACCEPTANCE_PASS' | ForEach-Object { Write-Host $_.Line } }
            break
        }
        if ([DateTime]::UtcNow -gt $deadline) { throw "Timed out waiting for $jobName. Inspect the Job before retrying." }
        Start-Sleep -Seconds 5
    }
}
Write-Host "PASS. Evidence retained at $evidence and $lakeRoot/acceptance.json. CronJob remains suspended for explicit promotion."
