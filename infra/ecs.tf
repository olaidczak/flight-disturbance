############ Sieć: domyślne VPC, publiczne podsieci ############
# Kontener potrzebuje tylko ruchu wychodzącego (websocket EMSC, SQS, ECR).
# Publiczne IP zamiast NAT Gateway, żeby nie płacić ~30 USD/mies.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "seismic_ws" {
  name        = "${var.project}-seismic-ws"
  description = "seismic_ws: tylko ruch wychodzacy"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

############ Rejestr obrazów ############
resource "aws_ecr_repository" "seismic_ws" {
  name                 = "${var.project}/seismic-ws"
  image_tag_mutability = "MUTABLE" # nadpisujemy tag "latest"
  force_delete         = true      # projekt studencki: destroy usuwa też obrazy

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "seismic_ws" {
  repository = aws_ecr_repository.seismic_ws.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Trzymaj 5 ostatnich obrazow"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

############ ECS ############
resource "aws_cloudwatch_log_group" "seismic_ws" {
  name              = "/ecs/${var.project}-seismic-ws"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "main" {
  name = var.project
}

resource "aws_ecs_task_definition" "seismic_ws" {
  family                   = "${var.project}-seismic-ws"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.seismic_ws_task.arn

  # obraz budowany lokalnie na Macu z Apple Silicon -> arm64
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name      = "seismic-ws"
    image     = "${aws_ecr_repository.seismic_ws.repository_url}:${var.seismic_ws_image_tag}"
    essential = true
    environment = [
      { name = "QUEUE_URL", value = aws_sqs_queue.events.url },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.seismic_ws.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "seismic-ws"
      }
    }
  }])
}

# Jedno stale działające zadanie słuchające websocketa. Jeśli proces się zakończy,
# ECS uruchomi nowe zadanie.
resource "aws_ecs_service" "seismic_ws" {
  name            = "seismic-ws"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.seismic_ws.arn
  desired_count   = 0 # 1 = działa, 0 = zatrzymany
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.seismic_ws.id]
    assign_public_ip = true
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  depends_on = [aws_iam_role_policy.seismic_ws_task]
}
