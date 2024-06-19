output "conn_str_storage" {
    value = module.techhub_sa.sa_primary_connection_string
    sensitive = true
}