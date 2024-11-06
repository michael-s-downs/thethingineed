# Get output of Terraform
$terraformOutput = Get-Content "$(terraformOutput.jsonOutputVariablesPath)" | ConvertFrom-Json
$jsonObject = @{}

# Get properties and convert into
$azuredict = @{}

foreach ($prop in $terraformOutput.psobject.properties) {
    $value = $prop.Value.value
    $jsonObject[$($prop.Name)] = $value

    # Check if is an azure credential
    if ($prop.Name -eq "conn_str_storage") {
        $azuredict["conn_str_storage"] = $value
    } else {
        # Generate name env var
        $variableName = "TF_VAR_" +$($prop.Name).ToUpper()

        # Scape value of json
        $jsonString = $value | ConvertTo-Json -Compress
        $escapedJsonString = $jsonString -replace '"', '\"'

        # Set env var Azure DevOps
        Write-Host "##vso[task.setvariable variable=$variableName;issecret=true]$escapedJsonString"

        # Print result
        Write-Output "Env var setting: $variableName = $escapedJsonString"
    }
}

# Convert dictionary to JSON
$azurejson = $azuredict | ConvertTo-Json -Compress

# Set env var of azure credentials
Write-Host "##vso[task.setvariable variable=TF_VAR_SECRET_AZURE ;issecret=true]$azurejson"

# Print result
Write-Output "Env var setting: TF_VAR_CONN_STR_STORAGE_JSON = $azurejson"