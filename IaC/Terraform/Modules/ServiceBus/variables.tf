variable "rg" {
  type        = string
  description = "Service bus resource group."
}

variable "location" {
  type        = string
  description = "Service bus location."
}

variable "tier" {
  type        = string
  description = "Service bus tier."
}

variable "capacity" {
  type        = number
  description = "Service bus capacity."
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

variable "tags" {
  type = map(any)
}
