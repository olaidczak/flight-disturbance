terraform {
  required_version = ">= 1.10" # use_lockfile w backendzie S3 wymaga 1.10+

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }

  # Zdalny stan we wspólnym buckecie. Bucket tworzysz RĘCZNIE przed `terraform init`
  # (patrz README / komendy w czacie). Wartości tutaj nie mogą być zmiennymi.
  backend "s3" {
    bucket       = "flight-disruption-tfstate-592964949672"
    key          = "flight-disruption/terraform.tfstate"
    region       = "eu-north-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
