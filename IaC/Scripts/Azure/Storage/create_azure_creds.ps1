$terraformOutput = Get-Content "$(terraformOutput.jsonOutputVariablesPath)" | ConvertFrom-Json
$jsonObject = @{}

foreach($prop in $terraformOutput.psobject.properties) {
  $value = $prop.Value.value
  $jsonObject[$($prop.Name)] = $value
}

$jsonString = $jsonObject | ConvertTo-Json -Compress

$escapedJsonString = $jsonString -replace '"', '\"'

Write-Host "##vso[task.setvariable variable=TF_VAR_SECRET_AZURE;issecret=true]$escapedJsonString"
