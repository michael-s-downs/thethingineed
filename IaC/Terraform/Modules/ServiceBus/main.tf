resource "azurerm_servicebus_namespace" "techhub_sb" {
  name                = var.name
  location            = var.location
  resource_group_name = var.rg
  sku                 = var.tier
  capacity            = var.capacity
  tags                = var.tags
}

resource "azurerm_servicebus_namespace_authorization_rule" "azure_service_bus_namespace_authorization_rule" {
  name         = "${var.rg}-sb-auth"
  namespace_id = azurerm_servicebus_namespace.techhub_sb.id

  listen = true
  send   = true
  manage = true

  depends_on = [azurerm_servicebus_namespace.techhub_sb]

}

resource "azurerm_servicebus_queue" "queues" {
  for_each     = { for queue in var.service_bus_queues : "${azurerm_servicebus_namespace.techhub_sb.name}.${queue.name}" => queue }
  name         = "${var.rg}--q-${each.value.name}"
  namespace_id = azurerm_servicebus_namespace.techhub_sb.id

  enable_partitioning   = each.value.enable_partitioning
  requires_session      = each.value.requires_session
  max_size_in_megabytes = each.value.max_size_in_megabytes

  depends_on = [azurerm_servicebus_namespace.techhub_sb]
}

resource "azurerm_servicebus_queue_authorization_rule" "azure_service_bus_queue_authorization_rule" {
  for_each = { for queue in azurerm_servicebus_queue.queues : "${azurerm_servicebus_namespace.techhub_sb.name}.${queue.name}" => queue }
  name     = "ReadWriteAccess"
  queue_id = each.value.id

  listen = true
  send   = true
  manage = false

  depends_on = [azurerm_servicebus_namespace.techhub_sb, azurerm_servicebus_queue.queues]
}
