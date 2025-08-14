terraform {
  required_version = ">= 1.2 "
  backend "s3" {
    bucket         = "portpro-tf-states"
    region         = "us-east-1"
    key            = "portpro/dispatch-optimize-svc.tfstate"
    dynamodb_table = "terraform-up-and-running-locks"
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.82.1"
    }
  }
}