# Define variables
storageNameDestiny=$TF_VAR_RG_NAME
destinyPath=$TF_VAR_MODELS_CONFIG
filePath=$TF_VAR_FILE
modelsConfig=$TF_VAR_FILE_MODELS_CONFIG

# Define Storage destiny
storageNameDestinyKey=$(az storage account keys list --account-name $storageNameDestiny --query '[0].value' -o tsv)

# Define SAS tokens
end=$(date -u -d "600 minutes" '+%Y-%m-%dT%H:%MZ')
sasDestiny=$(az storage account generate-sas --account-key $storageNameDestinyKey --expiry $end --account-name $storageNameDestiny --https-only --permissions rwdlacupiyt --resource-types sco --services bfqt -o tsv)

echo "$modelsConfig" > $filePath


# Copy files
destination="https://$storageNameDestiny.blob.core.windows.net/$storageNameDestiny-backend/$destinyPath?$sasDestiny"

echo $destination
echo $filePath

azcopy copy "$filePath" "$destination" --overwrite=true