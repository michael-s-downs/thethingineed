output "sb_primary_connection_string" {
  value = data.azurerm_servicebus_namespace_authorization_rule.default.primary_connection_string
}
