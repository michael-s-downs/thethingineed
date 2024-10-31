blobNameOrigin=$TF_VAR_STATUS_STORAGE
blobNameDestiny=$TF_VAR_BLOB_DESTINY
folderNameOrigin=$TF_VAR_CONFIG_ORIGIN

aws s3 cp s3://$blobNameOrigin/$folderNameOrigin/src/ s3://$blobNameDestiny/$folderNameOrigin/src/ --recursive