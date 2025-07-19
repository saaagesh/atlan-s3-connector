# Atlan S3 Connector

This project provides a custom connector to discover, catalog, and build lineage for data that flows from PostgreSQL, through an AWS S3 bucket, to Snowflake. It is designed to be run as a standalone Python script that connects to your Atlan instance and dynamically creates the necessary assets and relationships.

## Features

- **S3 Object Discovery**: Scans a specified S3 bucket for CSV files.
- **Dynamic Asset Cataloging**: Creates or updates `S3Object` assets in Atlan for each discovered file.
- **Automated Lineage Building**:
    - Creates end-to-end object lineage from a PostgreSQL source table, through the S3 object, to a Snowflake destination table.
    - Establishes direct column-level lineage from PostgreSQL to Snowflake columns.
- **Idempotent Design**: The script can be run multiple times. It cleans up previously created lineage on each run to ensure the Atlan environment reflects the current state.
- **Configuration-Driven**: Easily configurable through a `.env` file and `config.py`.

## Prerequisites

- Python 3.8+
- An active Atlan instance
- AWS credentials with access to the target S3 bucket
- Access to the source PostgreSQL and destination Snowflake databases within Atlan

## Configuration

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd atlan_s3_connector
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up environment variables**:
    Create a `.env` file in the `atlan_s3_connector` directory and add your Atlan credentials:
    ```
    ATLAN_BASE_URL="<your-atlan-base-url>"
    ATLAN_API_KEY="<your-atlan-api-key>"
    ```

4.  **Review script configuration**:
    Open `config.py` to review and, if necessary, update the S3, PostgreSQL, and Snowflake connection details to match your environment.

## How to Run

To execute the connector and build the lineage, run the `main.py` script from within the `atlan_s3_connector` directory:

```bash
python main.py
```

The script will log its progress to the console and to a file named `atlan_s3_connector.log`.

## Project Structure

```
atlan_s3_connector/
├── main.py                # Main pipeline orchestrator
├── s3_connector.py        # Handles S3 discovery and cataloging
├── lineage_builder.py     # Builds table and column lineage
├── config.py              # All script configurations
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (credentials)
└── utils.py               # Utility functions
```

## Lineage Model

This connector creates a comprehensive lineage model in Atlan:

1.  **Object-Level Lineage**:
    - A `Process` is created for the `PostgreSQL Table -> S3 Object` relationship.
    - A `Process` is created for the `S3 Object -> Snowflake Table` relationship.
    - A consolidated `Process` is created for the end-to-end `PostgreSQL Table -> Snowflake Table` relationship.

2.  **Column-Level Lineage**:
    - A `ColumnProcess` is created for each mapped column pair from `PostgreSQL Column -> Snowflake Column`.
    - This column-level process is parented under the end-to-end table process for a clear and logical hierarchy.
