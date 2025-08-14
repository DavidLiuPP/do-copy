provider "aws" {
  region = terraform.workspace == "live-eu" ? "eu-north-1" : "us-east-1"
  assume_role {
    role_arn     = "arn:aws:iam::${local.account_id[terraform.workspace]}:role/deployment"
    session_name = "deployment"
  }
}

provider "aws" {
  alias  = "shared-account"
  region = terraform.workspace == "live-eu" ? "eu-north-1" : "us-east-1"
}