resource "aws_s3_bucket" "storages" {
  for_each = toset(var.container_names)
  bucket = "${var.rg}-${each.value}"

  tags = var.tags
}