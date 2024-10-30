resource "azurerm_redis_cache" "redis" {
  name                = "redis-${var.rg}-${var.location}"
  location            = var.location
  resource_group_name = var.rg
  capacity            = 0
  family              = "C"
  sku_name            = "Standard"
  enable_non_ssl_port = true

  tags = var.tags
}


resource "azurerm_private_endpoint" "pep_redis" {
  name                = "pep-${var.resource}-${var.rg}"
  location            = var.location_vnet
  resource_group_name = var.rg
  subnet_id           = data.azurerm_subnet.subnet.id

  private_service_connection {
    name                           = "pep-${var.resource}-${var.rg}-connection"
    private_connection_resource_id = azurerm_redis_cache.redis.id
    subresource_names              = [var.resource]
    is_manual_connection           = false
  }

  tags = var.tags

  depends_on = [azurerm_redis_cache.redis]
}

resource "azurerm_private_dns_a_record" "redis_recordset" {
  name                = azurerm_redis_cache.redis.name
  zone_name           = data.azurerm_private_dns_zone.dns_zone.name
  resource_group_name = var.rg_dns
  ttl                 = 3600
  records             = [azurerm_private_endpoint.pep_redis.private_service_connection[0].private_ip_address]

  depends_on = [azurerm_private_endpoint.pep_redis]
}
