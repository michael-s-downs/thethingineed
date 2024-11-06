output "di_key" {
  value = azurerm_cognitive_account.document_intelligence.primary_access_key
}

output "di_endpoint" {
  value = azurerm_cognitive_account.document_intelligence.endpoint
}
