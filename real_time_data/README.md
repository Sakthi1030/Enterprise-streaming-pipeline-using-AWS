# Enterprise Streaming Pipeline using AWS

A comprehensive real-time data streaming and analytics platform built with AWS services, Apache Airflow, Snowflake, and agentic AI. This project demonstrates a complete data engineering solution with infrastructure as code, data generation, orchestration, and intelligent analytics capabilities.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Phases](#project-phases)
- [Tools & Technologies](#tools--technologies)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Contributing](#contributing)

## 🏗️ Architecture Overview

The pipeline follows a multi-phase architecture:

```
Data Generation → AWS Kinesis → S3 → Airflow ETL → Snowflake → Agentic AI Analytics → API → Kubernetes
```

This creates a complete data flow from real-time streaming to AI-driven insights.

## 📊 Project Phases

### Phase 1: AWS Infrastructure (`phase_1_aws_infra/`)
- **Terraform Configuration**: Infrastructure as Code for AWS setup
- **IAM Policies**: Security policies for ECS tasks and services
- **PowerShell Scripts**: Automated AWS environment setup
- **Key Components**: 
  - Kinesis Data Streams
  - S3 Buckets
  - Lambda Functions
  - ECS for containerized workloads

### Phase 2: Data Generation (`phase_2_data_generation/`)
- **Master Data Generation**: Create base datasets (customers, products, stores, sales reps, campaigns)
- **Transaction Generation**: Real-time transaction data generation
- **Stream to Kinesis**: Push data to AWS Kinesis streams
- **Data Formats**: CSV, JSON, Parquet files
- **Generated Data**:
  - Customers dataset
  - Products catalog
  - Sales representatives
  - Marketing campaigns
  - Store locations
  - Real-time transactions

### Phase 3: Airflow Orchestration (`phase_3_airflow_orchestration/`)
- **DAG Pipelines**:
  - `data_generation_dag.py`: Triggers data generation tasks
  - `kinesis_to_s3_dag.py`: Streams data from Kinesis to S3
  - `s3_to_snowflake_dag.py`: Loads data from S3 to Snowflake
  - `agentic_ai_trigger_dag.py`: Triggers AI analytics on processed data

- **Docker Setup**:
  - Dockerfile with all dependencies
  - Docker Compose for multi-container orchestration
  - Custom Airflow environment configuration
  - Entry point scripts

- **Custom Operators**: Extended Airflow operators for specialized tasks

### Phase 4: Snowflake Data Warehouse (`phase_4_snowflake/`)
- **Schema Deployment**: Star schema for dimensional modeling
- **Database Setup**: Create warehouse, databases, and schemas
- **Tables**: COPY_INTO operations for data loading
- **SQL Views**: Dimensional views for analytics
- **Snowflake Hooks**: Custom Airflow integration with Snowflake

### Phase 5: Agentic AI (`phase_5_agentic_ai/`)
- **Multi-Agent System**:
  - `root_agent.py`: Main orchestrator
  - `sales_agent.py`: Sales analytics and insights
  - `marketing_agent.py`: Marketing campaign analysis
  - `anomaly_agent.py`: Anomaly detection and alerts

- **AI Capabilities**:
  - Snowflake query tools
  - Report generation
  - Alert generation
  - Vector store for embeddings
  - REST API for agent interactions

- **API Endpoints**: FastAPI-based agent interface
- **Vector Memory**: Embedding-based knowledge store

### Phase 6: Kubernetes Deployment (`phase_6_kubernetes/`)
- **Namespaces**: Separate namespaces for different services
- **Deployments**:
  - Agentic AI deployment with replicas
  - Airflow deployment configuration
  - Service definitions for inter-pod communication

- **Scaling**: Horizontal Pod Autoscaler (HPA) for dynamic scaling
- **Monitoring**: Prometheus and Grafana integration

### Phase 7: Power BI Integration (`phase_7_powerbi/`)
- **Snowflake Connection**: Direct connection to Snowflake data warehouse
- **Dashboards**: Business intelligence visualizations
- **Semantic Model**: Data model for Power BI
- **Refresh Schedule**: Automated data refresh configuration

## 🛠️ Tools & Technologies

### Cloud Platform
- **AWS** (Amazon Web Services)
  - Kinesis: Real-time data streaming
  - S3: Data lake storage
  - Lambda: Serverless computing
  - ECS: Container orchestration
  - IAM: Identity and Access Management

### Data Engineering & Orchestration
- **Apache Airflow**: Workflow orchestration and scheduling
- **Docker**: Containerization
- **Docker Compose**: Multi-container orchestration

### Infrastructure as Code
- **Terraform**: Infrastructure provisioning and management
- **PowerShell**: Scripting and automation

### Data Warehouse
- **Snowflake**: Cloud data warehouse
  - SQL for querying
  - Schema design and optimization
  - Data sharing capabilities

### AI & Machine Learning
- **LangChain/Claude API**: Agentic AI framework
- **Embeddings**: Vector store for similarity search
- **Multi-Agent Systems**: Specialized agents for different domains

### API & Web Framework
- **FastAPI**: High-performance REST API
- **Pydantic**: Data validation

### Business Intelligence
- **Power BI**: Dashboard and visualization tool
- **Semantic Models**: Data modeling for BI

### Container Orchestration
- **Kubernetes**: Container orchestration platform
  - Horizontal Pod Autoscaler (HPA)
  - Namespace management
  - Service discovery

### Monitoring & Observability
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization and alerting

### Programming Languages
- **Python 3.13**: Main programming language
- **SQL**: Data warehouse queries
- **HCL/Terraform**: Infrastructure definition
- **YAML**: Configuration management
- **JSON**: Data interchange format

### Data Formats
- **CSV**: Structured tabular data
- **JSON**: Semi-structured data
- **Parquet**: Columnar storage format
- **Avro**: Serialization format for streaming

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.8+** (Project uses 3.13)
- **Docker & Docker Compose**
- **Terraform**
- **Git**
- **AWS CLI** configured with credentials
- **kubectl** (for Kubernetes operations)
- **Snowflake Account** with appropriate permissions

### Required AWS Services
- Kinesis Streams
- S3 Buckets
- Lambda
- ECS
- CloudWatch

### Required External Services
- Snowflake Account
- Power BI Account/License
- GitHub Account (for repository)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/Sakthi1030/Enterprise-streaming-pipeline-using-AWS.git
cd Enterprise-streaming-pipeline-using-AWS
```

### 2. Configure AWS
```bash
# Configure AWS credentials
aws configure

# Deploy infrastructure using Terraform
cd phase_1_aws_infra/terraform
terraform init
terraform apply
```

### 3. Setup Python Environment
```bash
cd phase_5_agentic_ai
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Deploy Airflow
```bash
cd phase_3_airflow_orchestration/docker
docker-compose up -d
```

### 5. Deploy to Kubernetes
```bash
cd phase_6_kubernetes
kubectl apply -f namespaces.yaml
kubectl apply -f agentic-ai/
kubectl apply -f airflow/
```

### 6. Configure Snowflake
```bash
# Execute SQL scripts
cd phase_4_snowflake/sql
# Connect to Snowflake and run:
# - create_warehouse.sql
# - create_database.sql
# - create_tables.sql
```

### 7. Run Data Generation
```bash
cd phase_2_data_generation
python generate_master_data.py
python generate_transactions.py
python stream_to_kinesis.py
```

## 📂 Project Structure

```
real_time_data/
├── phase_1_aws_infra/           # AWS Infrastructure setup
│   ├── iam/                     # IAM policies
│   └── terraform/               # Terraform configurations
├── phase_2_data_generation/     # Data generation scripts
│   ├── generate_master_data.py
│   ├── generate_transactions.py
│   ├── stream_to_kinesis.py
│   └── data/                    # Generated datasets
├── phase_3_airflow_orchestration/  # Airflow DAGs and Docker
│   ├── dags/                    # Airflow DAGs
│   └── docker/                  # Docker setup
├── phase_4_snowflake/           # Snowflake configuration
│   ├── sql/                     # SQL scripts
│   ├── schemas/                 # Schema definitions
│   └── airflow_hooks/           # Snowflake integration
├── phase_5_agentic_ai/          # AI agents and API
│   ├── agents/                  # Agent implementations
│   ├── api/                     # FastAPI application
│   ├── tools/                   # Tool implementations
│   ├── memory/                  # Vector store & embeddings
│   └── docker/                  # Docker configuration
├── phase_6_kubernetes/          # Kubernetes manifests
│   ├── agentic-ai/              # AI deployment
│   ├── airflow/                 # Airflow deployment
│   └── monitoring/              # Prometheus & Grafana
└── phase_7_powerbi/             # Power BI configuration
    ├── connection/              # Snowflake integration
    ├── dashboards/              # BI dashboards
    └── datasets/                # Semantic models
```

## ⚙️ Configuration

### Airflow Configuration
Edit `phase_3_airflow_orchestration/docker/airflow.env`:
```
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://...
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300
```

### Snowflake Configuration
Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_DATABASE=your_database
```

### AWS Configuration
Configure credentials in AWS CLI or use IAM roles within AWS services:
```bash
aws configure --profile your-profile
```

### Kubernetes Configuration
Update `phase_6_kubernetes/namespaces.yaml` with your specific settings.

## 🚀 Deployment

### Local Development
1. Start Docker containers for Airflow
2. Run data generation scripts
3. Monitor Airflow UI at `http://localhost:8080`

### AWS Deployment
1. Deploy infrastructure with Terraform
2. Push Docker images to ECR
3. Deploy DAGs to Airflow
4. Configure Snowflake connections

### Kubernetes Deployment
1. Configure kubectl for your cluster
2. Apply namespace configuration
3. Deploy services and deployments
4. Monitor with Prometheus/Grafana

## 📈 Key Features

✅ **Real-time Data Streaming**: AWS Kinesis for low-latency data ingestion
✅ **Scalable ETL**: Apache Airflow for workflow orchestration
✅ **Cloud Data Warehouse**: Snowflake for analytics
✅ **AI Analytics**: Multi-agent system for intelligent insights
✅ **Infrastructure as Code**: Terraform for reproducible deployments
✅ **Container Ready**: Docker and Kubernetes support
✅ **Business Intelligence**: Power BI integration
✅ **Monitoring**: Prometheus and Grafana
✅ **RESTful API**: FastAPI for agent interaction
✅ **Vector Search**: Embeddings for semantic search

## 🔒 Security

- IAM policies for least privilege access
- Encrypted data in transit and at rest
- Secure credential management
- Network isolation with Kubernetes network policies
- Regular security updates for dependencies

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For issues and questions:
- Open an issue on GitHub
- Check existing documentation in phase directories
- Review Docker logs for deployment issues

## 🎯 Next Steps

- Implement additional AI agents for specific use cases
- Add more data sources and integration points
- Enhance monitoring and alerting
- Scale infrastructure for production workloads
- Implement data quality frameworks
- Add machine learning pipelines

---

**Built with ❤️ using AWS, Airflow, Snowflake, and AI**
