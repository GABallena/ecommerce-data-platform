
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project identifier used in resource naming"
  type        = string
  default     = "ecommerce-pipeline"
}

variable "pipeline_schedule" {
  description = "CloudWatch Events cron expression for daily pipeline run"
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "alert_email" {
  description = "Email address for pipeline failure alerts"
  type        = string
  default     = "data-team@example.com"
}

variable "ecr_image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}
