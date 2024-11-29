resource "aws_sqs_queue" "sqs" {
  for_each = { for queue in var.name_queues : queue.name => queue }

  name                       = "${var.rg}--q-${each.value.name}.fifo"
  fifo_queue                 = true
  visibility_timeout_seconds = each.value.visibility_timeout_seconds
  message_retention_seconds  = each.value.message_retention_seconds
  tags                       = var.tags
}
