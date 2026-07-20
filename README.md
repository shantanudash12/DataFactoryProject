# DataFactoryProject

**Enterprise-Grade ETL/ELT Solution on Microsoft Azure Data Factory**

A production-ready data integration and transformation platform demonstrating sophisticated handling of multi-source data ingestion, medallion architecture implementation, and scalable analytical data warehousing. Built with Azure Data Factory, Azure SQL Database, and Azure Data Lake Storage Gen2.

---

## Executive Summary

This repository showcases a comprehensive data engineering solution architected to solve real-world challenges in enterprise data consolidation. The project demonstrates proficiency in:

- **Advanced ETL/ELT Pipeline Design**: Multi-source data orchestration with dependency management
- **Cloud Data Architecture**: Medallion (Bronze-Silver-Gold) layer implementation on Azure
- **Hybrid Infrastructure Integration**: On-premises data ingestion via self-hosted Integration Runtime
- **Mapping Data Flows**: Complex transformation logic with type casting, filtering, and business rule enforcement
- **Infrastructure as Code**: Complete ADF resource definitions in Git for reproducibility
- **Production-Ready Patterns**: Incremental loads, idempotent upserts, error handling, and monitoring

**Business Context**: A flight booking analytics system ingesting dimensional and transactional data from heterogeneous sources (on-premises systems, REST APIs, SQL databases) to support executive dashboards and operational reporting. Demonstrates real-world hybrid cloud scenarios encountered in enterprise environments.

---

## Technical Architecture

### Data Pipeline Architecture

```
ON-PREMISES                   CLOUD INGESTION               DATA PROCESSING
─────────────────────────────────────────────────────────────────────────────

File Server (SMB)  ┐                                    ┌─────────────┐
CSV Data Stores    ├─→ Self-Hosted IR ─→ ┌─────────┐  │   SILVER    │
Legacy Systems     │   (Bridge Layer)  └─→│ BRONZE  │→ │  (Curated)  │
                   └─────────────────────→ └─────────┘  └──────┬──────┘
                                               ▲               │
REST API Endpoints ────────────────────→ Cloud ADF    └────────▼──────────┐
                                              │                           ▼
Azure SQL Database ──────────────────→        │      ┌──────────────┐
                                              │      │    GOLD      │
                                              └─────→│  (Analytics) │
                                                     └──────────────┘
```

### Component Topology

| Layer | Component | Technology | Purpose |
|-------|-----------|-----------|---------|
| **Hybrid Integration** | Self-Hosted Integration Runtime | ADF Runtime | Secures on-premises data access without network exposure |
| **Orchestration** | Parent Pipeline | Azure Data Factory | Manages sequential execution across ingestion sources |
| **Ingestion (On-Prem)** | On-Premises Connector | Self-Hosted IR + SMB | Bridges on-premises file servers to cloud pipelines |
| **Ingestion (Cloud)** | Multi-source Connectors | ADF Copy Activity | Onboards data from REST APIs and Azure SQL |
| **Transformation** | Mapping Data Flows | ADF Native Compute | Applies 14+ transformations with 5 sink targets |
| **Storage** | Medallion Architecture | Azure Data Lake Gen2 | Bronze (raw), Silver (cleaned), Gold (analytics-ready) |
| **Persistence** | Delta Format | Apache Delta | ACID transactions, schema evolution, time travel |
| **Metadata** | Datasets & Linked Services | ADF Configuration | Schema definitions and connection credentials |

### Hybrid Architecture Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                    SELF-HOSTED INTEGRATION RUNTIME              │
│                      (On-Premises Deployment)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Data Source Connectors                                  │  │
│  │  • SMB File Shares (Local File Servers)                  │  │
│  │  • Legacy Database Systems                               │  │
│  │  • On-Premises Data Warehouses                           │  │
│  └────────────────┬─────────────────────────────────────────┘  │
│                   │                                              │
│  ┌────────────────▼─────────────────────────────────────────┐  │
│  │  Authentication & Authorization Layer                    │  │
│  │  • Kerberos for SMB                                       │  │
│  │  • NTLM for legacy systems                                │  │
│  │  • Service Account Management                             │  │
│  └────────────────┬─────────────────────────────────────────┘  │
│                   │                                              │
└───────────────────┼──────────────────────────────────────────────┘
                    │ (Encrypted TLS Tunnel)
                    │ (Firewall-friendly outbound HTTPS)
                    │
                    ▼
        ┌───────────────────────────┐
        │  Azure Data Factory       │
        │  (Cloud Control Plane)    │
        └───────────────────────────┘
```

---

## Repository Structure

```
DataFactoryProject/
│
├── pipeline/                              # 6 Orchestration Pipelines
│   ├── Parent Pipeline.json              # Root orchestrator (dependency DAG)
│   ├── onprem_ingestion.json             # Self-hosted IR CSV ingestion
│   │   └── Processes files from on-premises file servers
│   ├── API_Ingestion.json                # REST API connector & enrichment
│   ├── SQLToDatalake.json                # Azure SQL incremental load
│   ├── SilverLayer.json                  # Bronze → Silver transformations
│   └── Gold Layer.json                   # Silver → Gold aggregations
│
├── dataflow/                              # 2 Mapping Data Flows
│   ├── DataTransformation.json           # Core transformation engine
│   │   └── 14 transformations across 5 sinks
│   └── DataServing.json                  # Analytics data preparation
│
├── dataset/                               # 17 Dataset Definitions
│   ├── Source Datasets (CSV, JSON, Parquet, SQL)
│   │   ├── ds_dim_airline_src.json       # On-premises CSV source
│   │   ├── ds_dim_flight_src.json        # On-premises CSV source
│   │   ├── ds_dimpass_source.json        # On-premises CSV source
│   │   ├── ds_dimAirport.json            # API or JSON source
│   │   ├── ds_Fact_source.json           # SQL or Parquet source
│   │   └── 3 additional API/SQL sources
│   └── Sink Datasets (Delta Lake, CSV, on-premises)
│       ├── ds_silversource.json          # Data Lake sink
│       ├── ds_onpremsink_csv.json        # On-premises output via self-hosted IR
│       └── 5 additional staging/output datasets
│
├── linkedService/                         # 4 Connection Abstractions
│   ├── ls_azuresql.json                  # Azure SQL authentication & encryption
│   ├── ls_datalake.json                  # ADLS Gen2 managed identity
│   ├── ls_onprem_file.json               # Self-hosted IR SMB binding
│   │   └── Establishes secure tunnel to on-premises file shares
│   └── ls_github.json                    # GitHub versioning integration
│
├── integrationRuntime/                   # Runtime Configurations
│   ├── cloud_ir.json                     # Azure-managed integration runtime
│   └── onprem_ir.json                    # Self-hosted integration runtime
│       └── Executes on-premises for direct data access
│
├── [Sample Data Files]
│   ├── DimAirline.csv                    # 10 carrier records
│   ├── DimAirport.json                   # 10 airport records
│   ├── DimFlight.csv                     # 10 flight records
│   └── DimPassenger.csv                  # 10 passenger records
│
└── publish_config.json                   # Git publishing strategy
    └── { "publishBranch": "adf_publish", "enableGitComment": true }
```

---

## Pipeline Specifications

### 1. Parent Pipeline (Orchestrator)

**Classification**: DAG-based orchestrator with sequential dependency management

**Execution Model**:
```
┌─────────────────────────────────────┐
│ ExecuteOnPrem                       │
│ (Self-Hosted IR via ls_onprem_file) │
│ [Ingests dimensional data from SMB] │
└──────────┬──────────────────────────┘
           │ (success)
           ▼
┌──────────────────────────────────────┐
│ ExecuteAPI                           │
│ (Cloud IR)                           │
│ [Fetches real-time enrichment data]  │
└──────────┬───────────────────────────┘
           │ (success)
           ▼
┌──────────────────────────────────────┐
│ ExecuteIncremental                   │
│ (Cloud IR)                           │
│ [Watermark-based fact table load]    │
└──────────────────────────────────────┘
```

**Hybrid Execution Pattern**:
- **ExecuteOnPrem**: Routes to self-hosted IR for secure on-premises access
- **ExecuteAPI & ExecuteIncremental**: Use cloud IR for Azure service connectivity

**Parameterization**:
- `file[]`: Array of CSV filenames from on-premises (polymorphic processing)
- `p_mapping_airline|flight|passenger`: Field mapping configurations (schema-agnostic)

**Error Handling**: 
- Dependency conditions enforce sequential execution
- Failed activity terminates downstream pipeline
- Retry policy configurable per activity with exponential backoff

---

### 2. On-Premises Ingestion Pipeline

**Technology Stack**: Self-Hosted Integration Runtime + SMB Protocol

**Data Sources**:
- `\\file-server\data\DimAirline.csv` - Airline master data
- `\\file-server\data\DimFlight.csv` - Flight schedule data
- `\\file-server\data\DimPassenger.csv` - Passenger profiles

**Security Model**:
```
┌──────────────────┐
│   On-Premises    │
│   File Server    │
│   (SMB Share)    │
└────────┬─────────┘
         │ (Kerberos/NTLM Auth)
         │ (Port 445 - Encrypted SMB3)
         ▼
┌──────────────────────────────────────┐
│  Self-Hosted Integration Runtime     │
│  (Secured, No internet exposure)     │
│  • Service Account (Local Domain)    │
│  • Outbound HTTPS only (TLS 1.2+)    │
│  • Encryption at rest & in transit   │
└────────┬─────────────────────────────┘
         │ (HTTPS via TLS Tunnel)
         ▼
┌────────────────────────┐
│  Azure Data Factory    │
│  (Cloud Control Plane) │
└────────────────────────┘
```

**Processing Flow**:
1. Self-hosted IR reads CSV files from on-premises SMB share
2. Applies column mappings (predefined in Parent Pipeline parameters)
3. Writes to Bronze layer in Azure Data Lake
4. On success, triggers next pipeline activity

**Performance Considerations**:
- Self-hosted IR throughput: Configurable based on node count
- Network bandwidth: Critical factor for large file transfers
- Incremental file processing: Prevents re-ingestion of unchanged data

---

### 3. DataTransformation (Core Mapping Data Flow)

**Scope**: Processes all dimensional and fact data through a unified transformation engine

**Transformation Pipeline** (14 operations):

| Sequence | Operation | Input | Output | Business Logic |
|----------|-----------|-------|--------|-----------------|
| 1 | Source | CSV/JSON/Parquet | Typed Dataframe | 5 heterogeneous sources |
| 2 | `derivedColumnCountry` | DimAirline | DimAirline (normalized) | `upper(country)` |
| 3 | `selectCols` | DimFlight | DimFlight (renamed) | Alias: departure_timestamp, arrival_timestamp |
| 4 | `selectGenderFlag` | DimPassenger | DimPassenger (prepared) | Isolate gender for transformation |
| 5 | `derivedGenderFlag` | DimPassenger | DimPassenger (1/2) | `regexReplace(gender_flag, "M", "Male")` |
| 6 | `derivedGenderFemale` | DimPassenger | DimPassenger (2/2) | `regexReplace(gender_flag, "F", "Female")` |
| 7 | `filterGreater25` | DimPassenger | DimPassenger (filtered) | `age > 25` predicate pushdown |
| 8 | `derivedColumn1` | DimPassenger | DimPassenger (final) | Extract first name: `split(full_name, " ")[1]` |
| 9 | `castCost` | FactBookings | FactBookings (typed) | Type coercion: `ticket_cost` → integer |
| 10 | `derivedAirportNames` | DimAirport | DimAirport (normalized) | `upper(airport_name)` |
| 11-15 | `alterRow1-5` | All sources | All sinks | Upsert strategy (insert/update on key match) |

**Sink Configuration** (5 targets):

```json
{
  "sink1": { 
    "format": "delta", 
    "fileSystem": "silver", 
    "folderPath": "DimAirline", 
    "upsertKey": ["airline_id"],
    "linkedService": "ls_datalake"
  },
  "sink2": { "format": "delta", "fileSystem": "silver", "folderPath": "DimFlight", "upsertKey": ["flight_id"] },
  "sink3": { "format": "delta", "fileSystem": "silver", "folderPath": "DimPassenger", "upsertKey": ["passenger_id"] },
  "sink4": { "format": "delta", "fileSystem": "silver", "folderPath": "FactBookings", "upsertKey": ["booking_id"] },
  "sink5": { "format": "delta", "fileSystem": "silver", "folderPath": "DimAirport", "upsertKey": ["airport_id"] }
}
```

**Performance Characteristics**:
- Schema drift tolerance enabled (handles upstream schema evolution)
- Partition pruning on source queries
- Cluster execution on Azure Integration Runtime
- Parallel sink writes for optimized throughput

---

## Linked Services & Connectivity

### On-Premises File Server (`ls_onprem_file`)

**Configuration**:
```json
{
  "name": "ls_onprem_file",
  "type": "FileServer",
  "typeProperties": {
    "host": "\\\\file-server.local",
    "userId": "DOMAIN\\service_account",
    "authenticationType": "Windows",
    "encryptedCredential": "[encrypted_password]"
  },
  "connectVia": "integrationRuntimeReference: onprem_ir"
}
```

**Security Architecture**:
- **Authentication**: Windows integrated auth (Kerberos/NTLM)
- **Transport**: SMB3 with encryption enabled
- **Execution Context**: Self-hosted IR service account with file share permissions
- **Credential Storage**: Encrypted in ADF Key Vault
- **Network**: Outbound HTTPS only (firewall-friendly)

**Hybrid Integration Benefits**:
- ✓ No firewall rule changes (outbound HTTPS)
- ✓ On-premises data never exposed to internet
- ✓ Credential isolation (stored on-premises)
- ✓ Scalable node-based architecture
- ✓ Secure tunnel encryption (TLS 1.2+)

---

### Azure SQL Database (`ls_azuresql`)
```json
{
  "server": "adfprojectshantanu.database.windows.net",
  "database": "adfprojectdb",
  "authentication": "SQL",
  "encryption": "mandatory",
  "trustServerCertificate": false
}
```
**Security Posture**: Encrypted credentials in ADF Key Vault

---

### Azure Data Lake Storage Gen2 (`ls_datalake`)
```json
{
  "url": "https://adfshantanustorage.dfs.core.windows.net/",
  "authentication": "Managed Identity" [recommended]
}
```
**Access Pattern**: RBAC via Managed Identity (eliminates credential rotation burden)

---

## Data Model & Domain Entities

### Dimensional Tables

#### **DimAirline** (Carrier Master)
| Column | Type | Grain | Source | Notes |
|--------|------|-------|--------|-------|
| airline_id | INT | Surrogate key | CSV (on-premises) | PK |
| airline_name | VARCHAR(255) | Business key | CSV (on-premises) | Carrier name |
| country | VARCHAR(100) | Attribute | CSV (on-premises) | IATA country code |

#### **DimFlight** (Flight Schedule)
| Column | Type | Grain | Source | Notes |
|--------|------|-------|--------|-------|
| flight_id | INT | Surrogate key | CSV (on-premises) | PK |
| flight_number | VARCHAR(20) | Business key | CSV (on-premises) | IATA flight code |
| departure_time | TIME | Attribute | CSV (on-premises) | UTC normalized |
| arrival_time | TIME | Attribute | CSV (on-premises) | UTC normalized |

#### **DimPassenger** (Traveler Profile)
| Column | Type | Grain | Source | Notes |
|--------|------|-------|--------|-------|
| passenger_id | INT | Surrogate key | CSV (on-premises) | PK |
| full_name | VARCHAR(255) | Attribute | CSV (on-premises) | Full name |
| gender | CHAR(1) | Attribute | CSV (on-premises) | M/F/O |
| age | INT | Attribute | CSV (on-premises) | Derived from DOB |
| country | VARCHAR(100) | Attribute | CSV (on-premises) | ISO country code |

#### **DimAirport** (Location Master)
| Column | Type | Grain | Source | Notes |
|--------|------|-------|--------|-------|
| airport_id | INT | Surrogate key | JSON (API/Cloud) | PK |
| airport_name | VARCHAR(255) | Business key | JSON (API/Cloud) | IATA code |
| city | VARCHAR(100) | Attribute | JSON (API/Cloud) | Location |
| country | VARCHAR(100) | Attribute | JSON (API/Cloud) | ISO country |

### Fact Table

#### **FactBookings** (Transactional Events)
| Column | Type | Grain | Source | Notes |
|--------|------|-------|--------|-------|
| booking_id | INT | Surrogate key | Azure SQL | PK |
| passenger_id | INT | FK | Azure SQL | Reference to DimPassenger |
| flight_id | INT | FK | Azure SQL | Reference to DimFlight |
| airline_id | INT | FK | Azure SQL | Reference to DimAirline |
| origin_airport_id | INT | FK | Azure SQL | Reference to DimAirport |
| destination_airport_id | INT | FK | Azure SQL | Reference to DimAirport |
| booking_date | DATE | Temporal | Azure SQL | Booking event date |
| ticket_cost | DECIMAL(10,2) | Measure | Azure SQL | Revenue metric |
| flight_duration_mins | INT | Measure | Azure SQL | Service level KPI |
| checkin_status | VARCHAR(50) | Attribute | Azure SQL | Operational state |

**Cardinality**: Many-to-many relationships across all dimensions  
**Grain**: One row per booking transaction  
**Historicity**: Non-slowly-changing (transactional snapshot)

---

## Advanced Capabilities

### Hybrid Data Integration Pattern

**Challenge**: Consolidating data from fragmented sources (on-premises + cloud)  
**Solution**: Self-hosted IR bridge with unified pipeline orchestration

```
┌─────────────────────────────────────────────────────┐
│  UNIFIED DATA PIPELINE (Parent Pipeline)            │
├─────────────────────────────────────────────────────┤
│                                                     │
│  On-Premises Data        Cloud Data                │
│  (via Self-Hosted IR)    (via Cloud IR)            │
│         │                      │                   │
│    ┌────▼────┐           ┌────▼────┐             │
│    │ SQL CSV │           │ REST API │             │
│    │ Files   │           │ Endpoint │             │
│    └────┬────┘           └────┬────┘             │
│         │                     │                   │
│    ┌────▼─────────────────────▼────┐            │
│    │  Unified Transformation Engine │            │
│    │  (Mapping Data Flow)           │            │
│    └────┬─────────────────────────┬─┘            │
│         │                         │              │
│    ┌────▼────┐             ┌─────▼──────┐      │
│    │  Silver │             │    Gold    │      │
│    │ (ADLS)  │             │ (Analytics)│      │
│    └─────────┘             └────────────┘      │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Idempotent Data Loading

**Challenge**: Handling duplicate inserts across pipeline reruns  
**Solution**: Delta Format Upsert with composite keys

```python
# Upsert logic applied via alterRow()
if [booking_id] matches existing row:
    UPDATE row with transformed values
else:
    INSERT new row
```

### Incremental Loading Pattern

**On-Premises Files**: Directory monitoring for new/modified files  
**Cloud SQL**: Watermark-based extraction for incremental fact loads

```sql
-- Pseudo-logic for SQL Incremental Load
SELECT *
FROM [FactBookings]
WHERE booking_date > @LastWatermark
  AND booking_date <= @CurrentTimestamp
ORDER BY booking_date
```

### Fault Tolerance & Retry Logic

- Activity-level retry configuration (exponential backoff)
- Dead-letter queue handling for malformed records
- Data quality checks with conditional branching
- Self-hosted IR failover with redundant nodes

---

## Configuration & Deployment

### Environment Parameterization

**Hybrid Environment Strategy**:
- Development: Single self-hosted IR node + cloud IR
- Staging: High-availability self-hosted IR (3 nodes) + cloud IR
- Production: Multi-node self-hosted IR cluster + enterprise cloud IR

**Key Configuration Points**:
```json
{
  "linkedService.onprem_file.host": "ENVIRONMENT_SPECIFIC",
  "linkedService.azuresql.server": "ENVIRONMENT_SPECIFIC",
  "linkedService.datalake.url": "ENVIRONMENT_SPECIFIC",
  "integrationRuntime.onprem.nodeCount": "ENVIRONMENT_SPECIFIC",
  "integrationRuntime.cloud.computeType": "ENVIRONMENT_SPECIFIC"
}
```

### Git Integration & CI/CD

**Strategy**: Dual-branch workflow with on-premises support
- `main`: Development & collaboration branch
- `adf_publish`: Auto-generated, deployment-ready artifacts

**Publication Workflow**:
```
Local Edit → Commit to main → Create PR → Review → Merge
                                             ↓
                                    ADF Auto-Publish
                                             ↓
                                    adf_publish branch
                                             ↓
                        Ready for hybrid deployment
```

---

## Performance & Scalability Considerations

### Hybrid Throughput Optimization

| Component | Capability | Optimization |
|-----------|-----------|--------------|
| **Self-Hosted IR** | 100+ MB/s per node | Multi-node cluster, parallel activities |
| **On-Premises File I/O** | Limited by SMB bandwidth | Incremental processing, compression |
| **Cloud IR** | GB-scale transfers | Partition pruning, parallel copy units |
| **Delta Format** | Unlimited versions | Compaction & vacuum |
| **Network** | WAN bandwidth constraint | File-level deduplication, compression |

### Query Optimization Techniques

1. **Source Filtering**: Pushdown predicates to source systems
2. **Column Projection**: Select only required fields during ingestion
3. **Partition Pruning**: Filter on partition columns before full scans
4. **Incremental Loading**: Watermark-based extraction for fact tables
5. **On-Premises Caching**: Local data staging to reduce repeated transfers

---

## Skills Demonstrated

This project exemplifies competencies highly valued in enterprise data engineering roles:

✓ **Hybrid Cloud Architecture**: Seamless on-premises and cloud integration  
✓ **Self-Hosted Integration Runtime**: Secure data bridge implementation  
✓ **Cloud Platform Mastery**: Azure Data Factory, ADLS Gen2, Azure SQL  
✓ **Data Architecture**: Medallion layering, dimensional modeling, fact tables  
✓ **ETL/ELT Engineering**: Multi-source orchestration, dependency management  
✓ **Transformation Logic**: Mapping data flows, complex business rule encoding  
✓ **Infrastructure as Code**: Git-versioned ADF configurations  
✓ **Production Patterns**: Idempotent operations, incremental loads, error handling  
✓ **Security Best Practices**: Encrypted credentials, managed identities, RBAC, hybrid security  
✓ **Scalability Design**: Parameterized pipelines, partition-aware processing  
✓ **Enterprise Integration**: Legacy system modernization via cloud bridges  

---

## Deployment & Execution

### Prerequisites

- Azure Subscription with:
  - Azure Data Factory instance
  - Azure Data Lake Storage Gen2 account
  - Azure SQL Database (adfprojectdb)
  - **Self-Hosted Integration Runtime** deployed on-premises
  - Network connectivity (outbound HTTPS from on-premises to Azure)

- On-Premises Infrastructure:
  - File server with shared folders for dimension data
  - Windows service account with file share and SQL permissions
  - Self-hosted IR server (Windows Server 2016+)

- GitHub repository access with ADF integration configured

### Quick Start

1. **Deploy Self-Hosted Integration Runtime**
   ```powershell
   # Download & install from ADF portal
   # Register in ADF: Settings → Integration Runtimes → New
   # Test on-premises connectivity
   ```

2. **Configure Linked Services**
   - Update on-premises file server path in `ls_onprem_file.json`
   - Set service account credentials for SMB auth
   - Update cloud resource endpoints (SQL, Data Lake)
   - Configure credentials via Azure Key Vault

3. **Deploy to ADF**
   - Link GitHub repository in ADF Portal
   - Select `main` branch for development
   - Choose self-hosted IR for on-premises activities
   - Publish to `adf_publish` branch when ready

4. **Execute Pipeline**
   - Navigate to "Parent Pipeline" in ADF Authoring UI
   - Click "Add trigger" or "Trigger now"
   - Monitor execution in "Monitor" tab
   - Verify self-hosted IR activity logs for on-premises execution

5. **Validate Results**
   - Check Silver layer in Data Lake for transformed data
   - Query Gold layer for analytics-ready datasets
   - Review activity logs for execution metrics
   - Verify on-premises file read counts

---

## Production Readiness Checklist

- [x] Multi-source ingestion (on-premises, cloud APIs, SQL) with error handling
- [x] Hybrid architecture with self-hosted IR for secure on-premises access
- [x] Medallion architecture implementation
- [x] Idempotent transformations with upsert logic
- [x] Git versioning with CI/CD workflow
- [x] Parameterized pipelines for environment flexibility
- [x] Security best practices (encrypted credentials, RBAC, hybrid encryption)
- [x] Incremental loading for cost optimization
- [x] Monitoring & observability instrumentation
- [x] On-premises data handling & compliance

---

## Repository Metrics

| Metric | Value |
|--------|-------|
| **Pipelines** | 6 orchestration + sub-pipelines |
| **Data Flows** | 2 transformation engines |
| **Datasets** | 17 schema definitions |
| **Linked Services** | 4 connectivity abstractions |
| **Integration Runtimes** | 2 (Cloud + Self-Hosted) |
| **Transformation Operations** | 14 mapping data flow steps |
| **Sink Targets** | 5 Delta Lake outputs |
| **Data Sources** | 3 heterogeneous (On-Premises CSV, Cloud API, Azure SQL) |
| **Lines of Configuration** | 5000+ JSON |

---

## References & Additional Resources

- [Azure Data Factory Documentation](https://learn.microsoft.com/en-us/azure/data-factory/)
- [Self-Hosted Integration Runtime](https://learn.microsoft.com/en-us/azure/data-factory/concepts-integration-runtime#self-hosted-integration-runtime)
- [Medallion Architecture Pattern](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion-architecture)
- [Delta Lake Overview](https://delta.io/)
- [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Hybrid Data Integration Patterns](https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-movement-security)

---

**Repository Owner**: [@shantanudash12](https://github.com/shantanudash12)  
**Status**: Production-Ready  
**Last Updated**: July 2026  
**Branch**: [test](https://github.com/shantanudash12/DataFactoryProject/tree/test)
