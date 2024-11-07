resource "azurerm_servicebus_namespace" "sb" {
  name                = var.rg
  location            = var.location
  resource_group_name = var.rg
  sku                 = var.tier
  capacity            = var.capacity
  tags                = var.tags
}

data "azurerm_servicebus_namespace_authorization_rule" "default" {
  name         = "RootManageSharedAccessKey"
  namespace_id = azurerm_servicebus_namespace.sb.id

  depends_on = [azurerm_servicebus_namespace.sb]
}

resource "azurerm_servicebus_queue" "queues" {
  for_each     = { for queue in var.name_queues : "${azurerm_servicebus_namespace.sb.name}.${queue.name}" => queue }
  name         = "${var.rg}--q-${each.value.name}"
  namespace_id = azurerm_servicebus_namespace.sb.id

  enable_partitioning   = each.value.enable_partitioning
  requires_session      = each.value.requires_session
  max_size_in_megabytes = each.value.max_size_in_megabytes

  depends_on = [azurerm_servicebus_namespace.sb]
}

resource "azurerm_servicebus_queue_authorization_rule" "azure_service_bus_queue_authorization_rule" {
  for_each = { for queue in azurerm_servicebus_queue.queues : "${azurerm_servicebus_namespace.sb.name}.${queue.name}" => queue }
  name     = "ReadWriteAccess"
  queue_id = each.value.id

  listen = true
  send   = true
  manage = false

  depends_on = [azurerm_servicebus_namespace.sb, azurerm_servicebus_queue.queues]
}
