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

variable "SUBNET_GROUP" {
  type        = string
  description = "Name of subnet group"
}

variable "SECURITY_GROUP" {
  type        = string
  description = "Identifier of security group"
}

variable "SB_QUEUES" {
  type = list(object({
    name                       = string
    visibility_timeout_seconds = number
    message_retention_seconds  = number
  }))
  description = "Service bus list of queues"
}
