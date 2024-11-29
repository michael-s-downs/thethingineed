resource "random_password" "mysql_password" {
  length  = 32
  special = false
}

resource "aws_elasticache_subnet_group" "redis_subnet_group" {
  name       = var.rg
  subnet_ids = ["${var.subnet}"]

  tags = var.tags
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "redis-${var.rg}-${var.location}"
  engine               = "redis"
  engine_version       = "8.0"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis6.x"
  security_group_ids   = ["${var.vnet}"]
  subnet_group_name    = aws_elasticache_subnet_group.redis_subnet_group.name
  tags                 = var.tags

  depends_on = [aws_elasticache_subnet_group.redis_subnet_group]
}
