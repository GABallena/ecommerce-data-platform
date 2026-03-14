
resource "aws_ecr_repository" "pipeline" {
  name                 = "${var.project_name}"
  image_tag_mutability = "MUTABLE"
  force_delete         = var.environment != "prod"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = {
        type = "expire"
      }
    }]
  })
}

resource "aws_ecs_cluster" "pipeline" {
  name = "${var.project_name}-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/ecs/${var.project_name}-${var.environment}"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "pipeline" {
  family                   = "${var.project_name}-${var.environment}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.pipeline_task.arn

  container_definitions = jsonencode([{
    name  = "pipeline"
    image = "${aws_ecr_repository.pipeline.repository_url}:${var.ecr_image_tag}"

    essential = true

    environment = [
      { name = "PIPELINE_ENV", value = var.environment },
      { name = "AWS_REGION", value = var.aws_region },
      { name = "S3_RAW_BUCKET", value = aws_s3_bucket.raw_zone.id },
      { name = "S3_WAREHOUSE_BUCKET", value = aws_s3_bucket.warehouse.id },
      { name = "S3_LOGS_BUCKET", value = aws_s3_bucket.logs.id },
      { name = "SNS_ALERT_TOPIC", value = aws_sns_topic.pipeline_alerts.arn }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.pipeline.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "pipeline"
      }
    }

    command = ["python", "pipeline/run_pipeline.py"]
  }])
}

resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "${var.project_name}-daily-${var.environment}"
  description         = "Trigger pipeline daily"
  schedule_expression = var.pipeline_schedule
}

resource "aws_cloudwatch_event_target" "ecs_pipeline" {
  rule     = aws_cloudwatch_event_rule.daily_pipeline.name
  arn      = aws_ecs_cluster.pipeline.arn
  role_arn = aws_iam_role.events_ecs.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.pipeline.arn
    launch_type         = "FARGATE"
    task_count          = 1

    network_configuration {
      subnets          = data.aws_subnets.private.ids
      security_groups  = [aws_security_group.pipeline.id]
      assign_public_ip = false
    }
  }
}

resource "aws_iam_role" "events_ecs" {
  name = "${var.project_name}-events-ecs-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "events_ecs" {
  name = "${var.project_name}-events-run-task-${var.environment}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = aws_ecs_task_definition.pipeline.arn
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.pipeline_task.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "events_ecs" {
  role       = aws_iam_role.events_ecs.name
  policy_arn = aws_iam_policy.events_ecs.arn
}

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-alerts-${var.environment}"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "pipeline" {
  name        = "${var.project_name}-sg-${var.environment}"
  description = "Pipeline ECS tasks — egress only"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound for S3, ECR, CloudWatch"
  }
}
