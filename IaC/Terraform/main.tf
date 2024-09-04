terraform {
  required_version = ">= 1.3.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.36.0"
    }

  }
  backend "azurerm" {
    resource_group_name  = var.status_rg
    storage_account_name = var.status_storage
    container_name       = var.status_container_sa
    key                  = var.status_key_sa
  }
}

provider "azurerm" {
  features {}
}

locals {
  common_tags = {
    "Environment" = "Sandbox"
    "Deployment"  = "TechHub"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = var.RG_NAME
  location = var.RG_LOCATION
  tags     = local.common_tags
}

module "techhub_sa" {
  source           = "./Modules/StorageAccount"
  rg               = var.RG_NAME
  location         = var.RG_LOCATION
  tier             = var.SA_TIER
  replication_type = var.SA_REPLICATION_TYPE
  container_names  = var.SA_CONTAINER_NAMES
  tags             = local.common_tags
  depends_on       = [azurerm_resource_group.rg]
}

module "techhub_sb" {
  source             = "./Modules/ServiceBus"
  rg                 = var.RG_NAME
  location           = var.RG_LOCATION
  tier               = var.SB_TIER
  capacity           = var.SB_CAPACITY
  service_bus_queues = var.SB_QUEUES
  tags               = local.common_tags
  depends_on         = [azurerm_resource_group.rg]
}
