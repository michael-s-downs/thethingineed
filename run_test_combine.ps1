$env:inTEGRATION_NAME = 'search'
$env:stORAGE_PERSIST_FOLDER = 'test' 
$rootPath = "C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services"
$directories = Get-ChildItem -Path $rootPath -Directory | Where-Object { $_.Name -like "techhub*" }
$coverage_combine = "coverage combine --keep"
$testCommand = "coverage run -m pytest --disable-warnings --ignore-glob=*common*"

foreach ($dir in $directories) {
    Set-Location $dir.FullName
    $env:PythonPath = 'C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services'
    if ($dir.FullName -eq "C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services\techhubintegrationreceiver"){
        $env:PythonPath = 'C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services\common'
    }
    if ($dir.FullName -eq "C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services\techhubintegrationsender"){
        $env:PythonPath = 'C:\Users\idegrego\Documents\techhub\TechHubGlobalToolkit\services\common'
    }
    if (Test-Path ".coveragerc") {
        Write-Host "Running tests in directory: $($dir.FullName)"
        Invoke-Expression $testCommand
        $dirName = $dir.Name
        $coverage_combine += " $dirName"
        $coverage_combine += "/.coverage"
        Set-Location $rootPath
    }
}
Write-Host "Running $($coverage_combine)"
Invoke-Expression $coverage_combine
