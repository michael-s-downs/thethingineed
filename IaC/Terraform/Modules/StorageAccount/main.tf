resource "azurerm_storage_account" "sa" {
  name                            = var.rg
  resource_group_name             = var.rg
  location                        = var.location
  account_tier                    = var.tier
  account_replication_type        = var.replication_type
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_container" "sa_container" {
  for_each              = toset(var.container_names)
  name                  = "${var.rg}-${each.value}"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"

  depends_on = [azurerm_storage_account.sa]
}

resource "azurerm_storage_queue" "queues" {
  count                = var.create_queues ? 0 : length(var.name_queues)
  name                 = "${var.rg}--q-${var.name_queues[count.index].name}"
  storage_account_name = azurerm_storage_account.sa.name

  depends_on = [azurerm_storage_account.sa]
}

resource "azurerm_private_endpoint" "pep_blob" {
  name                = "pep-${var.resource_blob}-${var.rg}"
  location            = var.location_vnet
  resource_group_name = var.rg
  subnet_id           = data.azurerm_subnet.subnet.id

  private_service_connection {
    name                           = "pep-${var.resource_blob}-${var.rg}-connection"
    private_connection_resource_id = azurerm_storage_account.sa.id
    subresource_names              = [var.resource_blob]
    is_manual_connection           = false
  }

  tags = var.tags

  depends_on = [azurerm_storage_account.sa]
}

resource "azurerm_private_dns_a_record" "blob_recordset" {
  name                = azurerm_storage_account.sa.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone_blob.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_blob.private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_blob]
}

resource "azurerm_private_endpoint" "pep_queue" {
  count               = var.create_queues ? 0 : 1
  name                = "pep-${var.resource_queue}-${var.rg}"
  location            = var.location_vnet
  resource_group_name = var.rg
  subnet_id           = data.azurerm_subnet.subnet.id

  private_service_connection {
    name                           = "pep-${var.resource_queue}-${var.rg}-connection"
    private_connection_resource_id = azurerm_storage_account.sa.id
    subresource_names              = [var.resource_queue]
    is_manual_connection           = false
  }

  tags = var.tags

  depends_on = [azurerm_storage_account.sa]
}

resource "azurerm_private_dns_a_record" "queue_recordset" {
  count               = var.create_queues ? 1 : 0
  name                = azurerm_storage_account.sa.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone_queue.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_queue[0].private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_queue]
}
