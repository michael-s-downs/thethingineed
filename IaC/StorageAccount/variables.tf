variable "RG_NAME" {
  type        = string
  description = "Resource group name."
}

variable "RG_LOCATION" {
  type        = string
  description = "Resource location."
}

variable "SA_NAME" {
  type        = string
  description = "The name of the TechHub storage account."
}

variable "SA_TIER" {
  type        = string
  description = "Defines the Tier to use for this storage account. Valid options are Standard and Premium. For FileStorage accounts only Premium is valid. Changing this forces a new resource to be created."
}

variable "SA_REPLICATION_TYPE" {
  type        = string
  description = "Defines the type of replication to use for this storage account. Valid options are LRS, GRS, RAGRS and ZRS."
}

variable "SA_CONTAINER_NAMES" {
  type        = list(string)
  description = "The name of the container to create in the storage account."
}
