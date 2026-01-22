
# Disney Content Analytics Pipeline

[![Airflow](https://img.shields.io/badge/Airflow-2.7.1-blue)](https://airflow.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-orange)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue)](https://docker.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-yellow)](https://python.org/)

A production-ready data pipeline for Disney content analytics, demonstrating modern data engineering practices with Airflow, MinIO, and Docker.

## Highlights

- **End-to-End Data Pipeline**: Raw data ingestion -> Processing -> Analytics -> Storage
- **Production Architecture**: Containerized, scalable, and maintainable
- **Modern Tech Stack**: Airflow, MinIO, Docker, PostgreSQL, Redis
- **Real-world Use Case**: Disney content analytics and processing

## Architecture

```
Data Sources -> MinIO S3 Storage -> Airflow Orchestration -> Data Processing
     |               |                  |
     v               v                  v
  APIs/Files     Raw Data Buckets    DAGs/Tasks
     |               |                  |
     v               v                  v
Processed Data -> Analytics & ML -> Business Intelligence -> Decision Making
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Deployment
```bash
# Clone and deploy
git clone https://github.com/daouda85/disney-content-pipeline.git
cd disney-content-pipeline

# Start the entire stack
docker-compose up -d

# Wait for initialization (2-3 minutes)
sleep 180
```

### Access Dashboards
- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **MinIO Console**: http://localhost:9001 (admin/password123)

## Demo Pipeline

1. **Access Airflow** at http://localhost:8080
2. **Enable DAGs**: Toggle `process_csv_pipeline` ON
3. **Trigger Run**: Click play button
4. **Monitor**: Watch real-time data processing
5. **Results**: Check MinIO for processed data

## Technical Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow 2.7.1 | Workflow management & scheduling |
| **Storage** | MinIO (S3-compatible) | Scalable object storage |
| **Processing** | Python, Pandas | Data transformation & analytics |
| **Infrastructure** | Docker, Docker Compose | Containerization & deployment |
| **Database** | PostgreSQL | Metadata storage |
| **Queue** | Redis | Task queue & caching |

## Disney Use Cases

This pipeline architecture supports:
- **Content Metadata Processing**: Movie/show information, ratings, genres
- **User Behavior Analytics**: Watch patterns, engagement metrics
- **Recommendation Engine**: ML-powered content suggestions
- **Business Intelligence**: Executive dashboards and reports

## Project Structure

```
disney-content-pipeline/
├── dags/                           # Data pipelines
│   ├── process_csv_pipeline.py     # Main processing workflow
│   ├── test_connection_fixed.py    # Health checks
│   └── test_fixed_connection.py    # Integration tests
├── docker-compose.yml              # Infrastructure as code
├── sample_data.csv                 # Demo dataset
└── README.md                       # This file
```

## Key Features

- Containerized Deployment - One-command setup
- Scalable Architecture - Production-ready design
- Data Quality Checks - Validation and monitoring
- Error Handling - Retry logic and alerting
- Modular Design - Easy to extend and maintain
- Documentation - Comprehensive guides and examples

## Author

**Daouda** - [GitHub](https://github.com/daouda85)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
```

