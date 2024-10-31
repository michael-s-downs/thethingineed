output "mysql_password" {
  value = random_password.mysql_password.result
}

output "mysql_host" {
  value = azurerm_mysql_flexible_server.mysql.fqdn
}
