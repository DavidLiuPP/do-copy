module "ecs-service" {
  source        = "s3::/portpro-tf-modules/1.0.85/aws-ecs-fargate-service.zip"
  name          = local.app_name
  environment   = terraform.workspace
  cluster_name  = terraform.workspace == "pre" ? data.terraform_remote_state.portpro.outputs.pre_cluster_name[0] : data.terraform_remote_state.portpro.outputs.cluster_name
  desired_count = 1
  domain        = local.domain

  container = {
    APP_PORT       = "8080",
    APP_NAME       = local.app_name
    image          = aws_ecr_repository.this.repository_url
    log_group      = aws_cloudwatch_log_group.this.name
    aws_log_region = terraform.workspace == "live-eu" ? "eu-north-1" : "us-east-1"
  }
  # ALB
  alb_arn           = terraform.workspace == "pre" || terraform.workspace == "pre-eu" ? data.terraform_remote_state.portpro.outputs.pre_alb_arn[0] : data.terraform_remote_state.portpro.outputs.ecs_alb_arn
  health_check_path = "/"
  sg                = [data.terraform_remote_state.portpro.outputs.web-server-sg] # required for fargate sg inbound rules
  listener_arn      = terraform.workspace == "pre" || terraform.workspace == "pre-eu" ? data.terraform_remote_state.portpro.outputs.pre_ecs_listener_arn[0] : data.terraform_remote_state.portpro.outputs.ecs_listener_arn
  matcher           = "200,302,404"

  # VPC
  vpc_id     = data.terraform_remote_state.portpro.outputs.vpc_id
  subnet_ids = data.terraform_remote_state.portpro.outputs.private_subnets

  tags = merge(
    local.common_tags,
    {
      "Name" = local.app_name
    }
  )
  autoscaling = true
  cpu         = 1024
  memory      = 2048

  cloudwatch_alarm = {
    "5xx" = terraform.workspace == "live" ? 1 : 0
  }
  sns_topic_arn = local.sns_topic[terraform.workspace]

}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${terraform.workspace}/dispatch-optimize-queue"
  retention_in_days = terraform.workspace == "live" ? 30 : 7
}