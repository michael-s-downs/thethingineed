# Define variables
storageNameDestiny=$TF_VAR_SA_NAME
storageNameDestinyKey=$(az storage account keys list --account-name $storageNameDestiny --query '[0].value' -o tsv)
storageNameOrigin=$TF_VAR_SA_NAME_ORIGIN
storageNameOriginKey=$TF_VAR_SA_ORIGIN_KEY
promptsLLM=$TF_VAR_LLM_PROMPTS
modelsConfig=$TF_VAR_MODELS_CONFIG
templatesCompose=$TF_VAR_COMPOSE_TEMPLATES
blobNameDestiny=$TF_VAR_BLOB_DESTINY
blobNameOrigin=$TF_VAR_CONFIG_ORIGIN

# Define SAS tokens
end=$(date -u -d "600 minutes" '+%Y-%m-%dT%H:%MZ')
sasDestiny=$(az storage account generate-sas --account-key $storageNameDestinyKey --expiry $end --account-name $storageNameDestiny --https-only --permissions rwdlacupiyt --resource-types sco --services bfqt -o tsv)
sasOrigin=$(az storage account generate-sas --account-key $storageNameOriginKey --expiry $end --account-name $storageNameOrigin --https-only --permissions rwdlacupiyt --resource-types sco --services bfqt -o tsv)

# Declare files to copy
echo "Declaring files to copy"
declare -a filesToCopy=($promptsLLM $modelsConfig)

# Add templates compose
IFS=', ' read -r -a templatesComposeArray <<< "${templatesCompose//[\[\]\"]}"
filesToCopy+=("${templatesComposeArray[@]}")

echo "Copying files from $storageNameOrigin to $storageNameDestiny"
# Copy files
for file in "${filesToCopy[@]}"
do
    origin="https://$storageNameOrigin.blob.core.windows.net/$blobNameOrigin/$file?$sasOrigin"
    destination="https://$storageNameDestiny.blob.core.windows.net/$blobNameDestiny/$file?$sasDestiny"
    echo "Copying file $file from $origin to $destination"
    azcopy copy "$origin" "$destination" --overwrite=true
done

