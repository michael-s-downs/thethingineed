variable "rg" {
  type        = string
  description = "Resource group name."
}

variable "tags" {
  type = map(any)
}

variable "name_queues" {
  type = list(object({
    name                       = string
    visibility_timeout_seconds = number
    message_retention_seconds  = number
  }))
  description = "Service bus list of queues"
}
