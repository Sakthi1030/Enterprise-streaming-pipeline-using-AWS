# Kubernetes Deployment for Agentic AI System

This directory contains Kubernetes configuration files for deploying the Agentic AI System in a production environment.

## Architecture Overview

The system is deployed across multiple namespaces:

1. **ai-agents** - Agentic AI application and services
2. **data-pipeline** - Apache Airflow for orchestration
3. **monitoring** - Prometheus and Grafana for monitoring
4. **logging** - Log collection and aggregation (not implemented in this phase)

## Components

### 1. Agentic AI System (`agentic-ai/`)
- **API Service**: FastAPI application with multiple replicas
- **Celery Workers**: Asynchronous task processing
- **PostgreSQL**: Main database for agent memory and sessions
- **Redis**: Cache and message broker
- **Monitoring Agent**: Continuous anomaly detection
- **HPA**: Horizontal Pod Autoscaler for automatic scaling
- **Ingress**: External access with load balancing

### 2. Apache Airflow (`airflow/`)
- **Webserver**: Airflow UI and REST API
- **Scheduler**: DAG scheduling
- **Workers**: Task execution
- **PostgreSQL**: Metadata database
- **Redis**: Celery broker
- **Flower**: Celery monitoring
- **Ingress**: External access

### 3. Monitoring Stack (`monitoring/`)
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Visualization and dashboards
- **Alert Rules**: Pre-configured alerting rules

## Prerequisites

1. Kubernetes cluster (version 1.24+)
2. Helm (for additional chart installations)
3. kubectl configured to access the cluster
4. StorageClass configured in the cluster
5. NGINX Ingress Controller
6. Cert-Manager (for TLS certificates)

## Deployment Steps

### 1. Create Namespaces
```bash
kubectl apply -f namespaces.yaml