variable "rg" {
  type        = string
  description = "Resource group name."
}

variable "location" {
  type        = string
  description = "Resource location."
}

variable "tags" {
  type = map(any)
}


variable "name" {
  type        = string
  description = "The name of the TechHub storage account."
}

variable "tier" {
  type        = string
  description = "Defines the Tier to use for this storage account. Valid options are Standard and Premium. For FileStorage accounts only Premium is valid. Changing this forces a new resource to be created."
}

variable "replication_type" {
  type        = string
  description = "Defines the type of replication to use for this storage account. Valid options are LRS, GRS, RAGRS and ZRS."
}

variable "container_names" {
  type        = list(string)
  description = "The names of the container to create in the storage account."
}
