output "mysql_password" {
  value = random_password.mysql_password.result
}

output "mysql_host" {
    value = split(":", aws_db_instance.mysql.endpoint)[0]
}