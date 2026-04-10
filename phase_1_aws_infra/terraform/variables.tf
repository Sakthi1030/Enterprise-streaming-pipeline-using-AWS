variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.20.0/24"]
}

variable "enable_nat_gateway" {
  description = "Enable NAT Gateway"
  type        = bool
  default     = true
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards"
  type        = number
  default     = 2
}

variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "airflow_cpu" {
  description = "Airflow ECS task CPU units"
  type        = number
  default     = 1024
}

variable "airflow_memory" {
  description = "Airflow ECS task memory (MB)"
  type        = number
  default     = 2048
}

variable "airflow_image" {
  description = "Airflow Docker image"
  type        = string
  default     = "apache/airflow"
}

variable "airflow_version" {
  description = "Airflow version"
  type        = string
  default     = "2.7.3"
}

variable "airflow_fernet_key" {
  description = "Airflow Fernet key"
  type        = string
  default     = ""
}

variable "airflow_domain_name" {
  description = "Airflow domain name"
  type        = string
  default     = "airflow.sales-platform.local"
}