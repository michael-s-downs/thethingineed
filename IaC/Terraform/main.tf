terraform {
  required_version = ">= 1.3.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.36.0"
    }

  }
  # backend "azurerm" {
  #   resource_group_name  = var.status_rg
  #   storage_account_name = var.status_storage
  #   container_name       = var.status_container_sa
  #   key                  = var.status_key_sa
  # }
}

provider "azurerm" {
  features {}
}

locals {
  common_tags = {
    "Environment" = "Sandbox"
    "Deployment"  = "TechHub"
  }

  privates_dns_zones = {
    "blob"        = "privatelink.blob.core.windows.net"
    "queue"       = "privatelink.queue.core.windows.net"
    "mysqlServer" = "privatelink.mysql.database.azure.com"
    "redisCache"  = "privatelink.redis.cache.windows.net"
    "mysqlServer" = "privatelink.mysql.database.azure.com"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = var.RG_NAME
  location = var.RG_LOCATION
  tags     = local.common_tags
}

module "techhub_sa" {
  source            = "./Modules/StorageAccount"
  rg                = var.RG_NAME
  location          = var.RG_LOCATION
  location_vnet     = var.RG_LOCATION_VNET
  tier              = var.SA_TIER
  replication_type  = var.SA_REPLICATION_TYPE
  container_names   = var.SA_CONTAINER_NAMES
  tags              = local.common_tags
  private_dns_zones = local.privates_dns_zones
  rg_dns            = var.RG_NAME_DNS
  vnet              = var.VNET_NAME
  resource_blob     = "blob"
  resource_queue    = "queue"
  subnet            = var.RG_SUBNET
  create_queues     = var.CREATE_SB
  name_queues       = var.SB_QUEUES

  depends_on = [azurerm_resource_group.rg]
}

module "techhub_sb" {
  count       = var.CREATE_SB ? 1 : 0
  source      = "./Modules/ServiceBus"
  rg          = var.RG_NAME
  location    = var.RG_LOCATION
  tier        = var.SB_TIER
  capacity    = var.SB_CAPACITY
  name_queues = var.SB_QUEUES
  tags        = local.common_tags

  depends_on = [azurerm_resource_group.rg]
}

module "techhub_mysql" {
  source            = "./Modules/MySQL"
  rg                = var.RG_NAME
  location          = var.RG_LOCATION
  location_vnet     = var.RG_LOCATION_VNET
  tags              = local.common_tags
  private_dns_zones = local.privates_dns_zones
  rg_dns            = var.RG_NAME_DNS
  vnet              = var.VNET_NAME
  resource          = "mysqlServer"
  subnet            = var.RG_SUBNET

  depends_on = [azurerm_resource_group.rg]
}

module "techhub_redis" {
  source            = "./Modules/RedisCache"
  rg                = var.RG_NAME
  location          = var.RG_LOCATION
  location_vnet     = var.RG_LOCATION_VNET
  tags              = local.common_tags
  private_dns_zones = local.privates_dns_zones
  rg_dns            = var.RG_NAME_DNS
  vnet              = var.VNET_NAME
  resource          = "redisCache"
  subnet            = var.RG_SUBNET

  depends_on = [azurerm_resource_group.rg]
}
