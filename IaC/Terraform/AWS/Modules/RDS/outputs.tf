output "mysql_password" {
  value = random_password.mysql_password.result
}

output "mysql_host" {
    value = aws_db_instance.mysql.endpoint
}