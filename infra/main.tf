terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "labadmin"
}

# --- S3 : entrepôt des artefacts ---
# bucket : nom mondialement unique, suffixé par le compte
resource "aws_s3_bucket" "artifacts" {
  bucket = "marketplace-artifacts-383025814770"
}

# versioning : chaque upload garde l'historique → rollback possible
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- SQS FIFO : commandes désirées (cloud → avions) ---
resource "aws_sqs_queue" "desired" {
  name                        = "marketplace-desired.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
}

# --- SQS FIFO : rapports d'état (avions → cloud) ---
resource "aws_sqs_queue" "status" {
  name                        = "marketplace-status.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
}

# --- IAM : l'identité du programme embarqué ---
resource "aws_iam_user" "edge_operator" {
  name = "edge-operator"
}

resource "aws_iam_user_policy" "edge_operator" {
  name = "edge-operator-least-privilege"
  user = aws_iam_user.edge_operator.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadArtifacts"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*"
        ]
      },
      {
        Sid      = "SendStatus"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.status.arn]
      },
      {
        Sid      = "ReceiveDesired"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = [aws_sqs_queue.desired.arn]
      }
    ]
  })
}

# clés statiques : l'opérateur s'authentifie sans interaction humaine
resource "aws_iam_access_key" "edge_operator" {
  user = aws_iam_user.edge_operator.name
}
