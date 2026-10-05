output "bucket_name" {
  value = aws_s3_bucket.artifacts.id
}

output "edge_operator_access_key" {
  value     = aws_iam_access_key.edge_operator.id
  sensitive = true
}

output "edge_operator_secret_key" {
  value     = aws_iam_access_key.edge_operator.secret
  sensitive = true
}

output "desired_queue_url" {
  value = aws_sqs_queue.desired.url
}

output "status_queue_url" {
  value = aws_sqs_queue.status.url
}
