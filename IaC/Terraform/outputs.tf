output "azure" {
  value = jsonencode({
    conn_str_storage = module.techhub_sa.sa_primary_connection_string
    conn_str_queue   = var.CREATE_SB ? module.techhub_sb[0].sb_primary_connection_string : null
  })
  sensitive = true
}

output "mysql_conn_str" {
  value     = "Server=${module.techhub_mysql.mysql_host};Port=3306;Database=uhis;User=${var.RG_NAME};Password=${module.techhub_mysql.mysql_password};"
  sensitive = true
}

output "redis" {
  value = jsonencode({
    host     = module.techhub_redis.redis_host
    password = module.techhub_redis.redis_pass
    port     = "6379"
  })
  sensitive = true
}

output "azure_ocr" {
  value = jsonencode({
    endpoint       = module.techhub_di.di_endpoint
    key_credential = module.techhub_di.di_key
  })
  sensitive = true
}
