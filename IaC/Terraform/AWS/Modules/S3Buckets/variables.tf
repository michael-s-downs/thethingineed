variable "rg" {
  type        = string
  description = "Resource group name."
}

variable "container_names" {
  type        = list(string)
  description = "The names of the container to create in the storage account."
}

variable "tags" {
  type = map(any)
}
