terraform {
  required_version = ">= 1.3.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.73.0"
    }
  }

  backend "s3" {
    bucket = var.status_storage
    key    = "${var.status_container_sa}/${var.status_key_sa}"
    region = var.location
  }

}

provider "aws" {
  region = var.RG_LOCATION
  assume_role {
    role_arn = var.ROLE_AWS
  }
}

locals {
  common_tags = {
    "Environment"   = "Sandbox"
    "Deployment"    = "TechHub"
    "ResourceGroup" = var.RG_NAME
  }

  # privates_dns_zones = {
  #   "blob"  = "privatelink.blob.core.windows.net"
  #   "mysqlServer" = "privatelink.mysql.database.azure.com"
  # }
}

module "techhub_bucket" {
  source          = "./Modules/S3Buckets"
  rg              = var.RG_NAME
  container_names = var.SA_CONTAINER_NAMES
  tags            = local.common_tags
}

module "techhub_rds" {
  source   = "./Modules/RDS"
  rg       = var.RG_NAME
  location = var.RG_LOCATION
  subnet   = var.RG_SUBNET["rds"]
  vnet     = var.VNET_NAME["rds"]
  tags     = local.common_tags
}

module "techhub_redis" {
  source   = "./Modules/RedisCache"
  rg       = var.RG_NAME
  location = var.RG_LOCATION
  subnet   = var.RG_SUBNET["redis"]
  vnet     = var.VNET_NAME["redis"]
  tags     = local.common_tags
}

module "techhub_sqs" {
  source      = "./Modules/SQS"
  rg          = var.RG_NAME
  name_queues = var.SB_QUEUES
  tags        = local.common_tags
}
