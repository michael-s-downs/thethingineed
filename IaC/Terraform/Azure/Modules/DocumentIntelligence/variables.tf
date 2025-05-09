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

variable "resource" {
  type        = string
  description = "Type of resource to set"
}

variable "subnet" {
  type        = string
  description = "Name of subnet"
}

variable "private_endpoint" {
  type        = bool
  description = "Variable to decide if create private endpoint or not"
}
