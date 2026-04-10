output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "kinesis_stream_name" {
  description = "Kinesis stream name"
  value       = aws_kinesis_stream.sales_data.name
}

output "firehose_stream_name" {
  description = "Firehose stream name"
  value       = aws_kinesis_firehose_delivery_stream.sales_to_s3.name
}

output "raw_data_bucket" {
  description = "Raw data S3 bucket name"
  value       = aws_s3_bucket.raw_data.bucket
}

output "processed_data_bucket" {
  description = "Processed data S3 bucket name"
  value       = aws_s3_bucket.processed_data.bucket
}

output "airflow_data_bucket" {
  description = "Airflow data S3 bucket name"
  value       = aws_s3_bucket.airflow_data.bucket
}

output "rds_endpoint" {
  description = "RDS endpoint"
  value       = aws_db_instance.airflow.endpoint
}

output "rds_username" {
  description = "RDS username"
  value       = aws_db_instance.airflow.username
  sensitive   = true
}

output "rds_password" {
  description = "RDS password"
  value       = random_password.rds_password.result
  sensitive   = true
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "alb_dns_name" {
  description = "ALB DNS name"
  value       = aws_lb.airflow.dns_name
}

output "acm_certificate_arn" {
  description = "ACM certificate ARN"
  value       = aws_acm_certificate.airflow.arn
}

output "airflow_web_url" {
  description = "Airflow web URL"
  value       = "https://${var.airflow_domain_name}"
}

output "kinesis_security_group_id" {
  description = "Kinesis security group ID"
  value       = aws_security_group.kinesis.id
}

output "ecs_security_group_id" {
  description = "ECS security group ID"
  value       = aws_security_group.ecs.id
}

output "alb_security_group_id" {
  description = "ALB security group ID"
  value       = aws_security_group.alb.id
}

output "rds_security_group_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds.id
}

output "firehose_role_arn" {
  description = "Firehose IAM role ARN"
  value       = aws_iam_role.firehose.arn
}

output "ecs_execution_role_arn" {
  description = "ECS execution role ARN"
  value       = aws_iam_role.ecs_execution.arn
}

output "ecs_task_role_arn" {
  description = "ECS task role ARN"
  value       = aws_iam_role.ecs_task.arn
}

output "dynamodb_lock_table" {
  description = "DynamoDB lock table name"
  value       = aws_dynamodb_table.terraform_state_lock.name
}

output "environment_summary" {
  description = "Environment deployment summary"
  value = <<EOF
Real-Time Sales Platform - ${var.environment} Environment
=======================================================

Infrastructure deployed in ${var.aws_region}

Access Points:
- Airflow UI: https://${var.aws_lb.airflow.dns_name}
- Airflow Domain: https://${var.airflow_domain_name}

Data Streams:
- Kinesis Stream: ${aws_kinesis_stream.sales_data.name}
- Firehose: ${aws_kinesis_firehose_delivery_stream.sales_to_s3.name}

Storage:
- Raw Data: ${aws_s3_bucket.raw_data.bucket}
- Processed Data: ${aws_s3_bucket.processed_data.bucket}
- Airflow Data: ${aws_s3_bucket.airflow_data.bucket}

Database:
- RDS Endpoint: ${aws_db_instance.airflow.endpoint}
- Database: airflow
- Username: airflow

Compute:
- ECS Cluster: ${aws_ecs_cluster.main.name}
- ECS Service: ${aws_ecs_service.airflow.name}

Network:
- VPC: ${aws_vpc.main.id}
- Public Subnets: ${join(", ", aws_subnet.public[*].id)}
- Private Subnets: ${join(", ", aws_subnet.private[*].id)}

Security Groups:
- Kinesis: ${aws_security_group.kinesis.id}
- ECS: ${aws_security_group.ecs.id}
- ALB: ${aws_security_group.alb.id}
- RDS: ${aws_security_group.rds.id}

IAM Roles:
- Firehose: ${aws_iam_role.firehose.arn}
- ECS Execution: ${aws_iam_role.ecs_execution.arn}
- ECS Task: ${aws_iam_role.ecs_task.arn}

Next Steps:
1. Configure DNS for ${var.airflow_domain_name} to point to ${aws_lb.airflow.dns_name}
2. Complete ACM certificate validation
3. Access Airflow UI and configure connections
4. Proceed to Phase 2: Data Generation

EOF
}