# ETL Pipeline for ECtHR Cases Using Astro

This project provides an ETL (Extract, Transform, Load) pipeline that automates the process of downloading cases from the European Court of Human Rights (ECtHR), storing them in a Google Cloud Storage (GCS) bucket, and subsequently transferring the data to BigQuery for analysis. The pipeline is implemented using Apache Airflow and managed with Astro.

## Prerequisites

Before setting up and running the pipeline, ensure you have the following:

- **Astro CLI**: Installed on your local machine.
- **Google Cloud Platform (GCP) Account**: With permissions to create and manage GCS buckets and BigQuery datasets.
- **Service Account Key**: A JSON key file for a GCP service account with the necessary permissions.

## Setup Instructions

Follow these steps to set up and run the ETL pipeline:

1. **Clone the Repository**:

   ```bash
   git clone <repository_url>
   ```

2. **Initialize the Astro Project**:

Navigate to the project directory and initialize the Astro project:

   ```bash
   cd <project_directory>
   astro dev init
   ```

3. **Configure GCP Connection in Airflow**:

Set up a connection in Airflow to authenticate with GCP:

   - Access the Airflow UI by running:

  ```bash
  astro dev start
  ```

  - Navigate to Admin > Connections.

  - Create a new connection with the following details:

      * Conn Id: google_cloud_default

      * Conn Type: Google Cloud

      * Keyfile Path: Path to your service account JSON key file.

      * Project Id: Your GCP project ID.

For more details, refer to Astronomer's guide on creating a BigQuery connection in Airflow.

4. **Set Airflow Variables**:

Define the required variables in Airflow:

  - doc_type: Set to either "JUDGMENTS" or "DECISIONS".

  - language: Set to either "ENG" or "FRE".

These variables determine the type and language of the ECtHR cases to process.

5. **Deploy DAGs and Include Files**:

Copy your DAG files and any necessary auxiliary files to the appropriate directories:

  - DAGs: Place your DAG Python files in the dags directory of your Astro project.

  - Include Files: If you have additional scripts or data files, place them in the include directory.

This organization ensures that Airflow can access and execute your workflows correctly.

6. **Run the Airflow Scheduler and Webserver**:

Start the Airflow services:

  ```bash
  astro dev start
  ```

This command launches the Airflow scheduler and webserver, allowing you to manage and monitor your DAGs.

7. **Trigger the DAGs**:

Access the Airflow UI by navigating to http://localhost:8080 in your web browser.

  - Locate the ecthr_etl_pipeline DAG and trigger it. This DAG downloads the specified ECtHR cases and uploads them to the GCS bucket.

  - After the first DAG completes successfully, trigger the create_bigquery_table DAG to transfer the data from GCS to BigQuery.

Monitor the progress and logs of each DAG to ensure successful execution.
   



