locals {
  day_plan_app_name = "dayplan-do-${terraform.workspace}"

  day_plan_domains = {
    dev     = ""
    pre     = ""
    live    = "dayplan-do.app.portpro.io"
    live-eu = ""
  }
  day_plan_domain = local.day_plan_domains[terraform.workspace]
}

module "day-plan-ecs-service" {
  source        = "s3::/portpro-tf-modules/1.0.96/aws-ecs-fargate-service.zip"
  name          = local.day_plan_app_name
  environment   = terraform.workspace
  cluster_name  = terraform.workspace == "pre" ? data.terraform_remote_state.portpro.outputs.pre_cluster_name[0] : data.terraform_remote_state.portpro.outputs.cluster_name
  desired_count = 8
  domain        = local.day_plan_domain

  container = {
    APP_PORT       = "8080",
    APP_NAME       = local.day_plan_app_name
    image          = aws_ecr_repository.day-plan.repository_url
    log_group      = aws_cloudwatch_log_group.day-plan.name
    aws_log_region = terraform.workspace == "live-eu" ? "eu-north-1" : "us-east-1"
  }
  # ALB
  alb_arn           = terraform.workspace == "pre" || terraform.workspace == "pre-eu" ? data.terraform_remote_state.portpro.outputs.pre_alb_arn[0] : data.terraform_remote_state.portpro.outputs.ecs_alb_arn
  health_check_path = "/"
  sg                = [data.terraform_remote_state.portpro.outputs.web-server-sg] # required for fargate sg inbound rules
  listener_arn      = terraform.workspace == "pre" || terraform.workspace == "pre-eu" ? data.terraform_remote_state.portpro.outputs.pre_ecs_listener_arn[0] : data.terraform_remote_state.portpro.outputs.ecs_listener_arn
  matcher           = "200,302,404"

  # VPC
  vpc_id      = data.terraform_remote_state.portpro.outputs.vpc_id
  subnet_ids  = data.terraform_remote_state.portpro.outputs.private_subnets
  kms_key_arn = data.terraform_remote_state.portpro.outputs.portpro_kms_key_arn

  tags = merge(
    local.common_tags,
    {
      "Name" = local.day_plan_app_name
    }
  )
  autoscaling = true
  cpu         = 4096
  memory      = 23552

  cloudwatch_alarm = {
    "5xx" = terraform.workspace == "live" ? 1 : 0
  }
  sns_topic_arn = local.sns_topic[terraform.workspace]

}

resource "aws_cloudwatch_log_group" "day-plan" {
  name              = "/ecs/${terraform.workspace}/day-plan-dispatch-optimize"
  retention_in_days = terraform.workspace == "live" ? 90 : 7
}
