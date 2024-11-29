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

variable "vnet" {
  type        = string
  description = "Name of virtual network"
}


variable "subnet" {
  type        = string
  description = "Name of subnet"
}
