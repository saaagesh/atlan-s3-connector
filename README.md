# Atlan S3 Connector

A custom connector for integrating AWS S3 with Atlan, providing end-to-end data lineage, PII detection, and compliance tagging.

## Overview

The Atlan S3 Connector bridges the gap in Atlan's native connector ecosystem by providing a custom S3 connector that establishes end-to-end data lineage from PostgreSQL → S3 → Snowflake → Looker, with advanced AI-powered metadata enhancement capabilities.

### Key Features

- **Complete Pipeline Visibility**: End-to-end lineage tracking across the entire ETL pipeline
- **AI-Enhanced Metadata**: Automated PII detection, compliance tagging, and intelligent descriptions
- **Regulatory Compliance**: Built-in support for Singapore PDPA and Indonesian PP No. 71/2019
- **Operational Efficiency**: Automated cataloging with minimal manual intervention
- **Impact Analysis**: Clear visibility into downstream effects of schema changes

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │───▶│      AWS S3     │───▶│   Snowflake     │───▶│     Looker      │
│   (Source DB)   │    │  (Data Lake)    │    │ (Data Warehouse)│    │  (Analytics)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┘
                                 │                       │
                    ┌─────────────────────────────────────┐
                    │        ATLAN PLATFORM              │
                    │  ┌─────────────────────────────┐    │
                    │  │    Custom S3 Connector      │    │
                    │  │  • Asset Discovery          │    │
                    │  │  • Lineage Building         │    │
                    │  │  • AI Enhancement           │    │
                    │  │  • Compliance Tagging       │    │
                    │  └─────────────────────────────┘    │
                    └─────────────────────────────────────┘
```

## Components

1. **S3 Connector**: Discovers and catalogs S3 objects with schema inference
2. **Lineage Builder**: Establishes table and column-level lineage relationships
3. **AI Enhancer**: Uses Google Gemini for intelligent metadata generation and PII detection
4. **PII Classifier**: Detects and classifies PII data with CIA ratings
5. **PII Inventory**: Generates comprehensive inventory reports of PII data

## Setup

### Prerequisites

- Python 3.8+
- AWS account with S3 access
- Atlan instance with API access
- Google API key for Gemini AI (optional)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/atlan-s3-connector.git
   cd atlan-s3-connector
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables:
   - Create a `.env` file with the following variables:
     ```
     ATLAN_BASE_URL="https://your-atlan-instance.com"
     ATLAN_API_KEY="your-atlan-api-key"
     GEMINI_API_KEY="your-google-api-key"
     AWS_ACCESS_KEY_ID="your-aws-access-key"
     AWS_SECRET_ACCESS_KEY="your-aws-secret-key"
     ```

5. Create required Atlan tags:
   - Create the following tags in your Atlan instance:
     - `PII` - General tag for Personally Identifiable Information
     - `PDPA` - For Singapore Personal Data Protection Act compliance
     - `GDPR` - For GDPR compliance
     - `Sensitive` - For sensitive data
     - `Financial` - For financial data
     - `Customer Data` - For customer-related information
     - `Personal Information` - For general personal information

## Usage

### Running the Connector

To run the connector with default settings:

```bash
python main.py
```

This will:
1. Discover S3 objects in the configured bucket
2. Catalog them in Atlan
3. Build lineage relationships with PostgreSQL and Snowflake
4. Use AI to generate descriptions and detect PII
5. Apply compliance tags and CIA ratings
6. Generate a PII inventory report

### Configuration

Edit the `config.py` file to customize:

- S3 bucket name and region
- Connection details for PostgreSQL and Snowflake
- AI enhancement settings
- Compliance tags

## Deployment Options

### Local Execution

Run the connector locally for testing or one-time execution:

```bash
python main.py
```

### Cloud Function Deployment

Deploy as a serverless function on AWS Lambda or Google Cloud Functions:

```bash
cd cloud_function_deployment
./deploy_with_aws.sh
```

### Modern UI

A Flask-based web interface is available for interactive usage:

```bash
cd modern_ui
python app.py
```

## PII Detection and Classification

The connector uses Google's Gemini AI to detect PII in your data assets. It identifies:

- Names (firstname, lastname, fullname, contactname)
- Contact info (email, phone)
- Location data (address, city, postalcode)
- IDs (customerid, userid, employeeid)
- Financial data (accountnumber, creditcard)

For each asset, it:
1. Analyzes column names and sample data
2. Identifies PII types
3. Assigns sensitivity levels
4. Applies appropriate compliance tags
5. Records CIA (Confidentiality, Integrity, Availability) ratings

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.