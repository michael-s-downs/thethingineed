resource "random_password" "mysql_password" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "rds_subnet_group" {
  name       = var.rg
  subnet_ids = ["${var.subnet}"]

  tags = var.tags
}

resource "aws_db_instance" "mysql" {
  allocated_storage      = 5
  db_name                = "mysql-${var.rg}-${var.location}"
  engine                 = "mysql"
  engine_version         = "8.0"
  instance_class         = "db.t3.micro"
  username               = var.rg
  password               = random_password.mysql_password.result
  parameter_group_name   = "default.mysql8.0"
  publicly_accessible    = true
  vpc_security_group_ids = ["${var.vnet}"]
  db_subnet_group_name   = aws_db_subnet_group.rds_subnet_group.name
  tags                   = var.tags

  depends_on = [aws_db_subnet_group.rds_subnet_group]
}
