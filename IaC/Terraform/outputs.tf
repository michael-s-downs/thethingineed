output "conn_str_storage" {
  value     = module.techhub_sa.sa_primary_connection_string
  sensitive = true
}

output "conn_str_queue" {
  value     = var.CREATE_SB ? module.techhub_sb[0].sb_primary_connection_string : null
  sensitive = true
}

output "mysql_conn_str" {
  value = "Server=${module.techhub_mysql.mysql_host};Port=3306;Database=uhis;User=${var.RG_NAME};Password=${module.techhub_mysql.mysql_password};"
  sensitive = true
}

output "redis_host" {
  value = module.techhub_redis.redis_host
}

output "redis_password" {
  value = module.techhub_redis.redis_pass
  sensitive = true
}