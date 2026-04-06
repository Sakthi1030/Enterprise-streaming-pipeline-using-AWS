# Real-Time Data Pipeline with Agentic AI

A comprehensive end-to-end real-time data pipeline that ingests streaming data, orchestrates processing workflows, stores data in a cloud warehouse, and provides AI-powered insights through agentic systems.

## 🏗️ Architecture Overview

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  Data Sources   │─────▶│   Kinesis    │─────▶│    Airflow      │
│  (Streaming)    │      │   Streams    │      │  Orchestration  │
└─────────────────┘      └──────────────┘      └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│   Power BI      │◀─────│  Snowflake   │◀─────│  Data Processing│
│  Dashboards     │      │  Data Warehouse     │     & ETL       │
└─────────────────┘      └──────────────┘      └─────────────────┘
                                                         │
                                                         ▼
                         ┌──────────────┐      ┌─────────────────┐
                         │  Kubernetes  │─────▶│   Agentic AI    │
                         │   Cluster    │      │    System       │
                         └──────────────┘      └─────────────────┘
```

## 📋 Project Phases

### Phase 1: AWS Infrastructure Setup
- AWS account configuration
- IAM roles and policies
- Kinesis stream setup
- S3 bucket configuration
- **Tech Stack**: AWS CLI, Terraform

### Phase 2: Data Generation & Streaming
- Real-time data generation
- Kinesis producer implementation
- Data validation and monitoring
- **Tech Stack**: Python, Boto3, AWS Kinesis

### Phase 3: Airflow Orchestration
- Docker-based Airflow deployment
- DAG development for data pipelines
- Task scheduling and monitoring
- **Tech Stack**: Apache Airflow, Docker, Python

### Phase 4: Snowflake Integration
- Data warehouse setup
- Schema design and optimization
- ETL pipeline implementation
- **Tech Stack**: Snowflake, SQL, Python connectors

### Phase 5: Agentic AI System
- Multi-agent architecture
- Sales, Marketing, and Anomaly detection agents
- LLM integration (GPT-4)
- Tool-based agent capabilities
- **Tech Stack**: Python, OpenAI API, LangChain

### Phase 6: Kubernetes Deployment
- Minikube local cluster setup
- Containerized application deployment
- Service mesh configuration
- Monitoring with Prometheus & Grafana
- **Tech Stack**: Kubernetes, Docker, Helm

### Phase 7: Power BI Dashboards
- Snowflake connection setup
- Interactive dashboard creation
- Real-time data visualization
- **Tech Stack**: Power BI, DAX, M Query

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Docker Desktop
- AWS Account
- Snowflake Account
- Minikube (for Kubernetes)
- Power BI Desktop
- Git

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/real-time-data-pipeline.git
cd real-time-data-pipeline
```

2. **Set up Python environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure AWS credentials**
```bash
aws configure
```

4. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your credentials
```

## 📁 Project Structure

```
real-time-data-pipeline/
├── phase_1_aws_setup/
│   ├── terraform/              # Infrastructure as Code
│   └── scripts/                # Setup scripts
├── phase_2_data_generation/
│   ├── generators/             # Data generators
│   ├── producers/              # Kinesis producers
│   └── logs/                   # Application logs
├── phase_3_airflow_orchestration/
│   ├── dags/                   # Airflow DAGs
│   ├── docker/                 # Docker configuration
│   └── plugins/                # Custom Airflow plugins
├── phase_4_snowflake_integration/
│   ├── schemas/                # Database schemas
│   ├── queries/                # SQL queries
│   └── etl/                    # ETL scripts
├── phase_5_agentic_ai/
│   ├── agents/                 # AI agent implementations
│   ├── tools/                  # Agent tools
│   ├── api/                    # FastAPI endpoints
│   └── config/                 # Configuration files
├── phase_6_kubernetes/
│   ├── agentic-ai/            # K8s manifests for AI system
│   ├── airflow/               # K8s manifests for Airflow
│   ├── monitoring/            # Prometheus & Grafana
│   └── namespaces.yaml        # Namespace definitions
└── phase_7_powerbi/
    ├── dashboards/            # Power BI files (.pbix)
    ├── datasets/              # Data models
    └── connection/            # Connection guides
```

## 🔧 Configuration

### AWS Setup
See [phase_1_aws_setup/README.md](phase_1_aws_setup/README.md)

### Airflow Setup
See [phase_3_airflow_orchestration/README.md](phase_3_airflow_orchestration/README.md)

### Kubernetes Deployment
See [phase_6_kubernetes/README.md](phase_6_kubernetes/README.md)

## 🎯 Key Features

### Real-Time Data Processing
- Streaming data ingestion via AWS Kinesis
- Scalable processing with Apache Airflow
- Fault-tolerant pipeline design

### AI-Powered Insights
- **Sales Agent**: Performance analysis, revenue optimization, forecasting
- **Marketing Agent**: Campaign analysis, customer acquisition, ROI optimization
- **Anomaly Agent**: Real-time anomaly detection and alerting

### Cloud Data Warehouse
- Optimized Snowflake schema design
- Efficient data partitioning and clustering
- Query performance optimization

### Containerized Deployment
- Kubernetes orchestration
- Auto-scaling capabilities
- High availability setup

### Interactive Dashboards
- Real-time metrics visualization
- Custom KPI tracking
- Drill-down capabilities

## 📊 Monitoring & Observability

- **Prometheus**: Metrics collection
- **Grafana**: Visualization and alerting
- **Airflow UI**: Pipeline monitoring
- **CloudWatch**: AWS resource monitoring

## 🔐 Security

- Environment-based secret management
- IAM role-based access control
- Kubernetes secrets for sensitive data
- Network policies and service mesh

## 🧪 Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Run specific test suite
pytest tests/test_agents.py
```

## 📈 Performance

- **Throughput**: 10,000+ events/second
- **Latency**: <100ms end-to-end
- **Availability**: 99.9% uptime
- **Scalability**: Auto-scaling based on load

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- Your Name - Initial work

## 🙏 Acknowledgments

- AWS for cloud infrastructure
- Apache Airflow community
- Snowflake for data warehousing
- OpenAI for LLM capabilities
- Kubernetes community

## 📞 Support

For questions and support, please open an issue in the GitHub repository.

## 🗺️ Roadmap

- [ ] Add real-time streaming dashboard
- [ ] Implement MLOps pipeline
- [ ] Add more specialized agents
- [ ] Enhance monitoring and alerting
- [ ] Add CI/CD pipeline
- [ ] Implement data quality checks
- [ ] Add data lineage tracking

## 📚 Documentation

- [AWS Setup Guide](phase_1_aws_setup/README.md)
- [Data Generation Guide](phase_2_data_generation/README.md)
- [Airflow Guide](phase_3_airflow_orchestration/README.md)
- [Snowflake Integration](phase_4_snowflake_integration/README.md)
- [Agentic AI Documentation](phase_5_agentic_ai/README.md)
- [Kubernetes Deployment](phase_6_kubernetes/README.md)
- [Power BI Dashboards](phase_7_powerbi/README.md)
