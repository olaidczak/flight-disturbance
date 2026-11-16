variable "project" {
  type    = string
  default = "flight-disruption"
}

variable "region" {
  type    = string
  default = "eu-north-1"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12" # musi się zgadzać z wersją w scripts/build_lambda.sh i lokalnym venv
}

variable "log_retention_days" {
  type    = number
  default = 14
}

# Które pollery mają działać wg harmonogramu. Reszta dostaje schedule w stanie DISABLED.
# Dopisuj kolejne, gdy kod danego pollera jest gotowy i sekret ma wartość.
variable "enabled_pollers" {
  type    = list(string)
  default = ["noaa_swpc"]
}

# Tag obrazu seismic_ws w ECR, który uruchamia serwis
variable "seismic_ws_image_tag" {
  type    = string
  default = "latest"
}
