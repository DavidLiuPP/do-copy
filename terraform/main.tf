locals {
  app_name = "dispatch-optimize-queue-${terraform.workspace}"
  ecr_name = local.domain

  domains = {
    dev     = ""
    pre     = ""
    live    = "dispatch-optimize-queue.app.portpro.io"
    live-eu = "dispatch-optimize-queue.eu.portpro.io"
  }
  domain = local.domains[terraform.workspace]

  account_id = {
    dev     = "338521073506"
    pre     = "218980707526"
    live    = "218980707526"
    live-eu = "218980707526"
  }

  sns_topic = {
    dev     = "",
    pre     = "",
    live    = ""
    live-eu = ""
  }

  common_tags = {
    terraform = "true"
    repo      = "https://github.com/PortPro-Technologies-Inc/portpro-dispatch-optimize-queues"
  }
  region  = terraform.workspace == "live-eu" || terraform.workspace == "pre-eu" ? "eu-north-1" : "us-east-1"
  zone_id = "Z038955926QDZEJ9JWZAP"
}

resource "aws_route53_record" "www" {
  count   = terraform.workspace == "dev" ? 1 : 0
  zone_id = local.zone_id
  name    = local.domain
  type    = "A"

  alias {
    name                   = data.terraform_remote_state.portpro.outputs.alb_dns_name #aws_elb.main.dns_name
    zone_id                = data.terraform_remote_state.portpro.outputs.alb_zone_id
    evaluate_target_health = true
  }
}