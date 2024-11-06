# Define variables
storageNameDestiny=$TF_VAR_RG_NAME
destinyPath=$TF_VAR_MODELS_CONFIG
filePath=$TF_VAR_FILE
modelsConfig=$TF_VAR_FILE_MODELS_CONFIG

echo "$modelsConfig" > $filePath

# Copy files
echo $destination
echo $filePath

aws s3 cp $filePath s3://$storageNameDestiny/$destinyPath