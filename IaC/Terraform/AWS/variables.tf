variable "RG_NAME" {
  type        = string
  description = "Resource group name."
}

variable "RG_LOCATION" {
  type        = string
  description = "Resource location."
}

variable "SA_CONTAINER_NAMES" {
  type        = list(string)
  description = "The name of the container to create in the storage account."
}

variable "RG_SUBNET" {
  type        = map(string)
  description = "Names of subnet by resource to deploy."
}

variable "VNET_NAME" {
  type        = map(string)
  description = "Names of virtual network by resource to deploy."
}

variable "ROLE_AWS" {
  type        = string
  description = "Name of role to assign to script"
}

variable "SB_QUEUES" {
  type = list(object({
    name                       = string
    visibility_timeout_seconds = number
    message_retention_seconds  = number
  }))
  description = "Service bus list of queues"
}
