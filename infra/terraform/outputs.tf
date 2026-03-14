
output "ecr_repository_url" {
  description = "ECR repository URL for pipeline image"
  value       = aws_ecr_repository.pipeline.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.pipeline.name
}

output "s3_raw_bucket" {
  description = "S3 bucket for raw zone data"
  value       = aws_s3_bucket.raw_zone.id
}

output "s3_warehouse_bucket" {
  description = "S3 bucket for warehouse snapshots"
  value       = aws_s3_bucket.warehouse.id
}

output "s3_logs_bucket" {
  description = "S3 bucket for pipeline logs"
  value       = aws_s3_bucket.logs.id
}

output "sns_alert_topic_arn" {
  description = "SNS topic ARN for pipeline alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "pipeline_task_role_arn" {
  description = "IAM role ARN assumed by pipeline containers"
  value       = aws_iam_role.pipeline_task.arn
}
