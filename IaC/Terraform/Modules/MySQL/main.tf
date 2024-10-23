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

  sku_name = "B_Standard_B1ms"
  version  = "8.0.21"
  zone     = 1

  tags = var.tags

  depends_on = [random_password.mysql_password]
}

resource "azurerm_mysql_flexible_server_configuration" "require_secure_transport" {
  name                = "require_secure_transport"
  value               = "OFF"
  resource_group_name = var.rg
  server_name         = azurerm_mysql_flexible_server.mysql.name

  depends_on = [azurerm_mysql_flexible_server.mysql]
}

resource "azurerm_private_endpoint" "pep_mysql" {
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
  name                = azurerm_mysql_flexible_server.mysql.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_mysql.private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_mysql]
}
