variable "rg" {
  type        = string
  description = "Resource group name."
}

variable "location" {
  type        = string
  description = "Resource location."
}

variable "location_vnet" {
  type        = string
  description = "Resource location of vnet."
}

variable "tags" {
  type = map(any)
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

variable "private_dns_zones" {
  type        = map(any)
  description = "Values of privates dns zones enabled"
}

variable "rg_dns" {
  type        = string
  description = "Value of rg where private dns zones are deployed"
}

variable "vnet" {
  type        = string
  description = "Name of virtual network"
}

variable "resource_blob" {
  type        = string
  description = "Type of resource to set"
}

variable "resource_queue" {
  type        = string
  description = "Type of resource to set"
}

variable "subnet" {
  type        = string
  description = "Name of subnet"
}

variable "create_queues" {
  type        = bool
  description = "Define if it is necesary create queues"
}

variable "name_queues" {
  type = list(object({
    name                  = string
    max_size_in_megabytes = number
    enable_partitioning   = bool
    requires_session      = bool
  }))
  description = "Names of queues."
}
