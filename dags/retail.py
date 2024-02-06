"""
Retail DAG Documentation

This DAG automates the data pipeline for processing retail data from a CSV file, transforming it, and generating reports using DBT.

Tasks:
1. upload_csv_to_gcs: Uploads a CSV file from the local filesystem to Google Cloud Storage (GCS).
2. create_retails_dataset: Creates an empty dataset named 'retail' in Google BigQuery.
3. gcs_to_raw: Loads data from the uploaded CSV file in GCS into a BigQuery table named 'raw_invoices' in the 'retail' dataset.
4. check_load: Performs data quality checks on the loaded raw data.
5. transform: Executes DBT transformations on the raw data to generate transformed data.
6. check_transform: Performs data quality checks on the transformed data.
7. report: Executes DBT models to generate reports based on the transformed data.
8. check_report: Performs data quality checks on the generated reports.

Dependencies:
- The DAG starts with uploading the CSV file to GCS, followed by creating the BigQuery dataset.
- The data is then loaded from GCS to BigQuery and undergoes transformation using DBT.
- After transformation, data quality checks are performed on both the raw and transformed data.
- Finally, reports are generated using DBT, and data quality checks are performed on the generated reports.

Note:
- The tasks 'check_load', 'check_transform', and 'check_report' are Python functions imported from include.soda.check_function module and perform custom data quality checks.
- The tasks 'transform', 'report', and 'check_report' are grouped under DbtTaskGroup for executing DBT transformations and generating reports.
- External Python task 'check_report' specifies the location of the Python executable using the 'python' parameter.

"""

from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from airflow.models.baseoperator import chain

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['retails'],
)
def retail():
    # Task to upload CSV file to GCS
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/Online_Retail.csv',
        dst='raw/Online_Retail.csv',
        bucket='dummy_bucket',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    # Task to create BigQuery dataset
    create_retails_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retails_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )
    
    # Task to load data from GCS to BigQuery
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://dummy_bucket/raw/Online_Retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=True,  # Use native support for CSV
    )

    # Python task to perform data quality checks on loaded data
    @task
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    # Group of tasks to execute DBT transformations
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    # Python task to perform data quality checks on transformed data
    @task
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    # Group of tasks to execute DBT models and generate reports
    report = DbtTaskGroup(
            group_id='report',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/report']
            )
        )

    # External Python task to perform data quality checks on generated reports
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)
    
    # Define task dependencies
    chain(
        upload_csv_to_gcs,
        create_retails_dataset,
        gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report()
    )

retail()
