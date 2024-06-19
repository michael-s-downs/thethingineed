output "conn_str_storage" {
  value     = module.techhub_sa.sa_primary_connection_string
  sensitive = true
}

output "conn_str_queue" {
  value     = module.techhub_sb.sb_primary_connection_string
  sensitive = true
}
