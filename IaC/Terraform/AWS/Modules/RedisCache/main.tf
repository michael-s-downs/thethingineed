resource "random_password" "mysql_password" {
  length  = 32
  special = false
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id               = "redis-${var.rg}"
  engine                   = "redis"
  engine_version           = "8.0"
  node_type                = "cache.t3.micro"
  num_cache_nodes          = 1
  parameter_group_name     = "default.redis6.x"
  security_group_ids       = [var.sg]
  snapshot_retention_limit = 0
  subnet_group_name        = var.subnet_group
  tags                     = var.tags

  depends_on = [random_password.mysql_password]
}
