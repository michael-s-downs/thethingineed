variable "RG_NAME" {
  type        = string
  description = "Resource group name."
}

variable "RG_LOCATION" {
  type        = string
  description = "Resource location."
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

variable "SB_TIER" {
  type        = string
  description = "Defines the Tier to use for this service bus namespace. Valid options are Basic, Standard or Premium."
}

variable "SB_CAPACITY" {
  type        = number
  description = "Service bus capacity. If sku is Premium can be 1,2,3,4,8 or 16 when sku is Basic or Standard the value is 0."
}

variable "SB_QUEUES" {
  type = list(object({
    name                  = string
    max_size_in_megabytes = number
    enable_partitioning   = bool
    requires_session      = bool
  }))
  description = "Service bus list of queues"
}
