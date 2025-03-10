resource "random_password" "mysql_password" {
  length  = 32
  special = false
}

resource "azurerm_mysql_flexible_server" "mysql" {
  name                = "mysql-${var.rg}-${var.location}"
  location            = var.location
  resource_group_name = var.rg

  administrator_login    = var.rg
  administrator_password = random_password.mysql_password.result

  sku_name = "GP_Standard_D2ads_v5"
  version  = "8.0.21"
  zone     = 1

  tags = var.tags

  depends_on = [random_password.mysql_password]
}

resource "azurerm_private_endpoint" "pep_mysql" {
  count               = var.private_endpoint ? 1 : 0
  name                = "pep-${var.resource}-${var.rg}"
  location            = var.location_vnet
  resource_group_name = var.rg
  subnet_id           = data.azurerm_subnet.subnet.id

  private_service_connection {
    name                           = "pep-${var.resource}-${var.rg}-connection"
    private_connection_resource_id = azurerm_mysql_flexible_server.mysql.id
    subresource_names              = [var.resource]
    is_manual_connection           = false
  }

  tags = var.tags

  depends_on = [azurerm_mysql_flexible_server.mysql]
}

resource "azurerm_private_dns_a_record" "mysql_recordset" {
  count               = var.private_endpoint ? 1 : 0
  name                = azurerm_mysql_flexible_server.mysql.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_mysql[0].private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_mysql]
}
