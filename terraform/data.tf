data "terraform_remote_state" "portpro" {
  backend = "s3"

  config = {
    bucket = "portpro-tf-states"
    key    = terraform.workspace == "live" || terraform.workspace == "pre" ? "env:/live/portpro-core.tfstate" : "env:/${terraform.workspace}/portpro-core.tfstate"
    region = "us-east-1"
  }
}