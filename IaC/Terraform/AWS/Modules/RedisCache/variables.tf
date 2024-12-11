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

variable "subnet_group" {
  type        = string
  description = "Name of subnet group"
}

variable "sg" {
  type        = string
  description = "Identifier of security group"
}
