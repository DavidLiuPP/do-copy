resource "aws_ecr_repository" "day-plan" {
  provider             = aws.shared-account
  name                 = local.day_plan_domain
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(
    local.common_tags
  )
}

resource "aws_ecr_lifecycle_policy" "day-plan-policy" {
  provider   = aws.shared-account
  repository = aws_ecr_repository.day-plan.name

  policy     = <<EOF
    {
        "rules": [
            {
                "rulePriority": 1,
                "description": "Expire tagged images and maintain last 10 latest images",
                "selection": {
                    "tagStatus": "any",
                    "countType": "imageCountMoreThan",
                    "countNumber": 10
                },
                "action": {
                    "type": "expire"
                }
            }
        ]
    }
    EOF
  depends_on = [aws_ecr_repository.day-plan]
}

resource "aws_ecr_repository_policy" "day-plan-policy" {
  repository = aws_ecr_repository.day-plan.name
  provider   = aws.shared-account

  policy     = <<EOF
    {
    "Version": "2008-10-17",
    "Statement": [
        {
        "Sid": "AllowCrossAccountPull",
        "Effect": "Allow",
        "Principal": {
            "AWS": [
            "arn:aws:iam::${local.account_id[terraform.workspace]}:root"
            ]
        },
        "Action": [
            "ecr:GetDownloadUrlForLayer",
            "ecr:BatchCheckLayerAvailability",
            "ecr:BatchGetImage"
        ]
        }
    ]
    }
    EOF
  depends_on = [aws_ecr_repository.day-plan]
}
