resource "aws_s3_bucket" "storages" {
  for_each = toset(var.container_names)
  bucket   = "${var.rg}-${each.value}"

  force_destroy = true

  tags = var.tags
}
