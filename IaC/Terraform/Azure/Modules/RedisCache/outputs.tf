output "redis_host" {
  value = azurerm_redis_cache.redis.hostname
}

output "redis_pass" {
  value = azurerm_redis_cache.redis.primary_access_key
}
