output "mysql_conn_str" {
  value     = "Server=${module.techhub_rds.mysql_host};Port=3306;Database=uhis;User=${var.RG_NAME};Password=${module.techhub_rds.mysql_password};"
  sensitive = true
}

output "redis" {
  value = jsonencode({
    host = module.techhub_redis.redis_host
    port = "6379"
  })
}