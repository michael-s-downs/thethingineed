resource "random_password" "mysql_password" {
  length  = 32
  special = false
}

resource "aws_db_instance" "mysql" {
  identifier             = "mysql-${var.rg}"
  allocated_storage      = 20
  engine                 = "mysql"
  engine_version         = "8.0"
  instance_class         = "db.t3.micro"
  username               = var.rg
  password               = random_password.mysql_password.result
  parameter_group_name   = "default.mysql8.0"
  publicly_accessible    = true
  vpc_security_group_ids = [var.sg]
  skip_final_snapshot    = true
  db_subnet_group_name   = var.subnet_group
  tags                   = var.tags

  depends_on = [random_password.mysql_password]
}
