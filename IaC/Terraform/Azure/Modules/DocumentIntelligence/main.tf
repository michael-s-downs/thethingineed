resource "azurerm_cognitive_account" "document_intelligence" {
  name                = var.rg
  location            = var.location
  resource_group_name = var.rg

  kind     = "FormRecognizer"
  sku_name = "S0"

  custom_subdomain_name = var.rg

  tags = var.tags
}

resource "azurerm_private_endpoint" "pep_di" {
  name                = "pep-${var.resource}-${var.rg}"
  location            = var.location_vnet
  resource_group_name = var.rg
  subnet_id           = data.azurerm_subnet.subnet.id

  private_service_connection {
    name                           = "pep-${var.resource}-${var.rg}-connection"
    private_connection_resource_id = azurerm_cognitive_account.document_intelligence.id
    subresource_names              = [var.resource]
    is_manual_connection           = false
  }

  tags = var.tags

  depends_on = [azurerm_cognitive_account.document_intelligence]
}

resource "azurerm_private_dns_a_record" "mysql_recordset" {
  name                = azurerm_cognitive_account.document_intelligence.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_di.private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_di]
}
