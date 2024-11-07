# Define variables
storageNameDestiny=$TF_VAR_RG_NAME
storageNameDestinyKey=$(az storage account keys list --account-name $storageNameDestiny --query '[0].value' -o tsv)
storageNameOrigin=$TF_VAR_STATUS_STORAGE
storageNameOriginKey=$TF_VAR_SA_ORIGIN_KEY
promptsLLM=$TF_VAR_LLM_PROMPTS
destinyPathLLM=$TF_VAR_MODELS_CONFIG_LLM
destinyPathIR=$TF_VAR_MODELS_CONFIG_IR
integrationModelMap=$TF_VAR_MODELS_MAP
embeddingModelDefault=$TF_VAR_EMBEDDING_MODELS
templatesCompose=$TF_VAR_COMPOSE_TEMPLATES
queryFiltersTemplates=$TF_VAR_QUERY_FILTERS_TEMPLATES
layoutConfig=$TF_VAR_LAYOUT_CONFIG
blobNameDestiny=$TF_VAR_BLOB_DESTINY
blobNameOrigin=$TF_VAR_CONFIG_ORIGIN

# Define new vars
modelsConfigLLM=$TF_VAR_MODELS_LLM
modelsConfigIR=$TF_VAR_MODELS_IR
filePathLLM=$TF_VAR_FILE_LLM
filePathIR=$TF_VAR_FILE_IR

# Define SAS tokens
end=$(date -u -d "600 minutes" '+%Y-%m-%dT%H:%MZ')
sasDestiny=$(az storage account generate-sas --account-key $storageNameDestinyKey --expiry $end --account-name $storageNameDestiny --https-only --permissions rwdlacupiyt --resource-types sco --services bfqt -o tsv)
sasOrigin=$(az storage account generate-sas --account-key $storageNameOriginKey --expiry $end --account-name $storageNameOrigin --https-only --permissions rwdlacupiyt --resource-types sco --services bfqt -o tsv)

# Declare files to copy
echo "Declaring files to copy"
declare -a filesToCopy=($layoutConfig $embeddingModelDefault $integrationModelMap)

# Add prompts LLM
IFS=', ' read -r -a promptsLLMArray <<< "${promptsLLM//[\[\]\"]}"
filesToCopy+=("${promptsLLMArray[@]}")

# Add templates compose
IFS=', ' read -r -a templatesComposeArray <<< "${templatesCompose//[\[\]\"]}"
filesToCopy+=("${templatesComposeArray[@]}")

# Add query filters compose
IFS=', ' read -r -a queryFiltersTemplatesArray <<< "${queryFiltersTemplates//[\[\]\"]}"
filesToCopy+=("${queryFiltersTemplatesArray[@]}")

echo "Copying files from $storageNameOrigin to $storageNameDestiny"
# Copy files
for file in "${filesToCopy[@]}"
do
    origin="https://$storageNameOrigin.blob.core.windows.net/$blobNameOrigin/$file?$sasOrigin"
    destination="https://$storageNameDestiny.blob.core.windows.net/$blobNameDestiny/$file?$sasDestiny"
    echo "Copying file $file from $origin to $destination"
    azcopy copy "$origin" "$destination" --overwrite=true
done

# Create models config in local
echo "$modelsConfigLLM" > $filePathLLM
echo "$modelsConfigIR" > $filePathIR

# Copy files
destinationLLM="https://$storageNameDestiny.blob.core.windows.net/$storageNameDestiny-backend/$destinyPathLLM?$sasDestiny"
destinationIR="https://$storageNameDestiny.blob.core.windows.net/$storageNameDestiny-backend/$destinyPathIR?$sasDestiny"

# Logger paths
echo $destinationLLM
echo $filePathLLM
echo $destinationIR
echo $filePathIR

# Copy files in blob
azcopy copy "$filePathLLM" "$destinationLLM" --overwrite=true
azcopy copy "$filePathIR" "$destinationIR" --overwrite=true