# Enterprise Streaming Pipeline using AWS

A comprehensive end-to-end real-time data streaming and analytics pipeline on AWS with agentic AI capabilities. This project demonstrates enterprise-grade infrastructure, data orchestration, analytics, and intelligent automation.

## 🏗️ Architecture Overview

The pipeline consists of 7 phases:

1. **AWS Infrastructure** - Terraform IaC and IAM configuration
2. **Real-time Data Generation** - Synthetic data streaming to AWS Kinesis
3. **Airflow Orchestration** - Data pipeline orchestration with Docker
4. **Snowflake Analytics** - Data warehouse and SQL transformations
5. **Agentic AI** - Multi-agent AI system for intelligent analysis
6. **Kubernetes Deployment** - Container orchestration and monitoring
7. **Power BI** - Business intelligence dashboards

## 🛠️ Tools & Technologies Used

### **Cloud Platform**
- **AWS** - Core cloud provider
  - S3 - Data storage and data lake
  - Kinesis - Real-time data streaming
  - IAM - Identity and access management
  - EC2/ECS - Compute resources

### **Infrastructure as Code**
- **Terraform** - Infrastructure provisioning and management
- **PowerShell** - AWS automation scripts

### **Data Integration & Streaming**
- **AWS Kinesis** - Real-time data ingestion
- **Apache Airflow** - Workflow orchestration and scheduling
- **Python** - Data pipelines and ETL

### **Data Warehouse**
- **Snowflake** - Cloud data warehouse
- **SQL** - Data transformation and analytics

### **Container & Orchestration**
- **Docker** - Containerization
  - Docker Compose - Multi-container orchestration
- **Kubernetes** - Production container orchestration
  - Prometheus - Metrics collection
  - Grafana - Monitoring and visualization
  - Horizontal Pod Autoscaler (HPA) - Request-based auto-scaling

### **Agentic AI & LLMs**
- **Python** - AI/ML implementation
- **LangChain** - LLM framework (implied by agent architecture)
- **Vector Store** - Embeddings and semantic search
- **Agents**:
  - Root Agent - Main orchestrator
  - Sales Agent - Sales analytics and insights
  - Marketing Agent - Marketing campaign analysis
  - Anomaly Agent - Anomaly detection and alerting

### **APIs & Backend**
- **FastAPI** - RESTful API framework
- **Pydantic** - Data validation and schema management

### **Business Intelligence**
- **Power BI** - Interactive dashboards and reporting
- **Snowflake Connector** - Direct data integration

### **Version Control**
- **Git** - Source code management
- **GitHub** - Repository hosting

### **Development & Environment Management**
- **Python 3.13** - Programming language
- **pip/venv** - Python dependency management
- **YAML** - Configuration management

## 📁 Project Structure

```
Enterprise-streaming-pipeline-using-AWS/
├── phase_1_aws_infra/          # Terraform IaC and IAM policies
│   ├── iam/                    # IAM roles and policies
│   └── terraform/              # AWS resource definitions
├── phase_2_data_generation/    # Real-time data generation
│   ├── generate_master_data.py # Master data creation
│   ├── generate_transactions.py# Transaction data generation
│   ├── stream_to_kinesis.py   # Kinesis streaming
│   └── data/                   # Sample data and metadata
├── phase_3_airflow_orchestration/ # Workflow orchestration
│   ├── dags/                   # Airflow DAGs
│   ├── docker/                 # Docker containerization
│   └── plugins/                # Custom Airflow operators
├── phase_4_snowflake/          # Data warehouse setup
│   ├── schemas/                # Star schema definitions
│   ├── sql/                    # SQL scripts
│   └── airflow_hooks/          # Snowflake integration
├── phase_5_agentic_ai/         # Multi-agent AI system
│   ├── agents/                 # AI agents (root, sales, marketing, anomaly)
│   ├── api/                    # FastAPI endpoints
│   ├── tools/                  # Agent tools (Snowflake, alerts, reports)
│   └── memory/                 # Vector store and embeddings
├── phase_6_kubernetes/         # K8s deployment configs
│   ├── agentic-ai/            # AI service deployment
│   ├── airflow/               # Airflow deployment
│   └── monitoring/            # Prometheus & Grafana
└── phase_7_powerbi/           # BI integration
    ├── connection/            # Snowflake connection setup
    └── dashboards/            # Power BI dashboards
```

## 🚀 Quick Start

### Prerequisites
- AWS Account with appropriate credentials
- Terraform installed
- Docker & Docker Compose
- Python 3.13+
- Kubernetes cluster (for Phase 6)
- Power BI Desktop/Cloud account
- Snowflake account

### Phase 1: AWS Infrastructure
```bash
cd phase_1_aws_infra/terraform/
# Configure variables.tf with your AWS account details
terraform init
terraform plan
terraform apply
```

### Phase 2: Data Generation
```bash
cd phase_2_data_generation/
pip install -r requirements.txt
python stream_to_kinesis.py
```

### Phase 3: Airflow Orchestration
```bash
cd phase_3_airflow_orchestration/docker/
docker-compose up -d
# Access Airflow UI at http://localhost:8080
```

### Phase 4: Snowflake Setup
```bash
cd phase_4_snowflake/
# Update connection credentials
python deploy_snowflake_schema.py
```

### Phase 5: Agentic AI
```bash
cd phase_5_agentic_ai/
python -m venv venv_313
source venv_313/bin/activate  # On Windows: venv_313\Scripts\activate
pip install -r requirements.txt
python api/main.py
# Access API at http://localhost:8000
```

### Phase 6: Kubernetes Deployment
```bash
cd phase_6_kubernetes/
kubectl apply -f namespaces.yaml
kubectl apply -f airflow/
kubectl apply -f agentic-ai/
kubectl apply -f monitoring/
```

### Phase 7: Power BI Integration
Follow the connection steps in `phase_7_powerbi/connection/snowflake_connection_steps.md`

## 📊 Key Features

- **Real-time Data Streaming** - Kinesis-based event ingestion
- **Automated ETL** - Airflow orchestration with error handling
- **Scalable Analytics** - Snowflake for petabyte-scale analysis
- **Intelligent Automation** - Multi-agent AI for insights
- **Production-Ready** - Kubernetes, monitoring, and high availability
- **Data Visualization** - Power BI dashboards for business insights

## 🤖 AI Agents

The agentic AI system includes:
- **Root Agent** - Coordinates all agent activities
- **Sales Agent** - Analyzes sales trends and performance
- **Marketing Agent** - Campaign optimization and ROI analysis
- **Anomaly Agent** - Detects and alerts on data anomalies

## 📈 Monitoring & Observability

- **Prometheus** - Metrics collection
- **Grafana** - Real-time dashboards and alerts
- **Airflow UI** - Pipeline monitoring and debugging
- **Snowflake Query History** - Query performance analysis

## 🔐 Security

- **IAM Policies** - Fine-grained access control
- **AWS Secrets Manager** - Credential management
- **Role-based Access** - Kubernetes RBAC
- **Encryption** - In-transit and at-rest

## 📝 Configuration Files

Key configuration files:
- `phase_3_airflow_orchestration/docker/airflow.env` - Airflow settings
- `phase_4_snowflake/schemas/star_schema.md` - Data schema documentation
- `phase_6_kubernetes/namespaces.yaml` - K8s namespace definitions

## 🧪 Testing

Each phase includes Docker containerization for isolated testing:
```bash
# Test Airflow DAGs
docker-compose -f phase_3_airflow_orchestration/docker/docker-compose.yml up

# Test AI agents
docker build -f phase_5_agentic_ai/docker/Dockerfile -t agentic-ai .
docker run agentic-ai
```

## 📚 Documentation

- [Phase 1 AWS Infrastructure](phase_1_aws_infra/README.md)
- [Phase 5 Agentic AI](phase_5_agentic_ai/README.md)
- [Phase 6 Kubernetes](phase_6_kubernetes/README.md)
- [Snowflake Schema](phase_4_snowflake/schemas/star_schema.md)

## 🛣️ Development Workflow

1. Clone repository
2. Configure AWS credentials (`~/.aws/credentials`)
3. Set up Python virtual environment
4. Deploy infrastructure with Terraform
5. Run data generation scripts
6. Start Airflow orchestration
7. Deploy to Kubernetes
8. Configure Power BI dashboards

## 🤝 Contributing

1. Test changes locally with Docker
2. Validate Terraform configurations
3. Update documentation
4. Create pull request

## 📄 License

This project is provided as-is for educational and enterprise use.

## 📞 Support

For issues and questions:
1. Check existing documentation
2. Review DAG logs in Airflow UI
3. Check Prometheus/Grafana metrics
4. Review application logs

---

**Last Updated:** April 2026

**Status:** ✅ Production-Ready
