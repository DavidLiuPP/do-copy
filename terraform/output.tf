output "task_execution_role_arn" {
  value = module.ecs-service.task_execution_role_arn
}

output "task_role_arn" {
  value = module.ecs-service.task_role_arn
}

output "service_name" {
  value = module.ecs-service.service_name
}
output "ecr" {
  value = aws_ecr_repository.this.repository_url
}