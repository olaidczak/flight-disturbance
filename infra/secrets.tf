# Terraform tworzy tylko "pojemniki". Wartości wpisujesz ręcznie (CLI/konsola),
# żeby klucze nie trafiły do kodu ani do tfstate.
resource "aws_secretsmanager_secret" "opensky" {
  name                    = "${var.project}/opensky" # JSON: {"client_id": "...", "client_secret": "..."}
  recovery_window_in_days = 0                         # projekt studencki: kasowanie od razu
}

resource "aws_secretsmanager_secret" "tomorrow_io" {
  name                    = "${var.project}/tomorrow-io" # JSON: {"api_keys": ["klucz1", "klucz2"]}
  recovery_window_in_days = 0
}
