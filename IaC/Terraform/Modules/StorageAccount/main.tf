resource "azurerm_storage_account" "techhub_sa" {
  name                            = var.name
  resource_group_name             = var.rg
  location                        = var.location
  account_tier                    = var.tier
  account_replication_type        = var.replication_type
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_container" "techhub_sa_container" {
  for_each              = toset(var.container_names)
  name                  = "${var.rg}-${each.value}"
  storage_account_name  = azurerm_storage_account.techhub_sa.name
  container_access_type = "private"
}
