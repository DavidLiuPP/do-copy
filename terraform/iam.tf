data "aws_iam_role" "task" {
  name = split("/", "${module.ecs-service.task_role_arn}")[1]
}

