
from google.cloud import bigquery
# from google.api_core import exceptions as GoogleExceptions

class QueryRunner:
    """
    Runs queries on BigQuery.
    
    Attributes:
        client: BigQuery client
        run_project: the project that the job will run on behalf of
        job_history: a list of handlers to query jobs ran
    """
    def __init__(self, client=None, run_project="stanleysfang"):
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(project=run_project)
        
        self.run_project = self.client.project
        self.job_history=[]
    
    def config_job(self, destination_table=None, overwrite=True, time_partitioning=False, partition_field=None, dry_run=False):
        job_config = bigquery.QueryJobConfig()
        
        job_config.use_legacy_sql = False
        job_config.destination = destination_table
        job_config.dry_run = dry_run
        
        if overwrite:
            job_config.write_disposition = 'WRITE_TRUNCATE'
        else:
            job_config.write_disposition = 'WRITE_APPEND'
        
        # The only supported partition type is "DAY" which is the default.
        if destination_table and time_partitioning:
            job_config.time_partitioning = bigquery.table.TimePartitioning(field=partition_field)
        
        return job_config
    
    def run_query(self, query_str, destination_table=None, overwrite=True, time_partitioning=False, partition_field=None, dry_run=False):
        """
        Executes a query and returns a handler to the job.
        
        Args:
            query_str: query string
            destination_table: the table that the query will write to (e.g. 'project_id.dataset_id.table_id') (for time partition tables, 'project_id.dataset_id.table_id$YYYYMMDD')
            overwrite: a boolean where True will overwrite destination table and False will append to it
            time_partitioning: a boolean where True will write the query to a partition table
            partition_field: the column to partition the table on (column can be TIMESTAMP or DATE data type)
            dry_run: a boolean indicating whether the query should be a dry run
        
        Returns:
            google.cloud.bigquery.job.QueryJob: a handler to the query job
        """
        job_config = self.config_job(destination_table, overwrite, time_partitioning, partition_field, dry_run)
        query_job = self.client.query(query_str, job_config=job_config)
        self.job_history.append(query_job)
        
        return query_job

class Loader:
    """
    Loads tables to BigQuery.
    
    Attributes:
        client: BigQuery client
        run_project: the project that the job will run on behalf of
        job_history: a list of handlers to load jobs ran
    """
    def __init__(self, client=None, run_project="stanleysfang"):
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(project=run_project)
        
        self.run_project = self.client.project
        self.job_history=[]
    
    def config_job(self, schema, overwrite=True, time_partitioning=False, partition_field=None):
        job_config = bigquery.LoadJobConfig()
        
        job_config.schema = [bigquery.SchemaField(name, field_type) for name, field_type in schema]
        
        if overwrite:
            job_config.write_disposition = 'WRITE_TRUNCATE'
        else:
            job_config.write_disposition = 'WRITE_APPEND'
        
        # The only supported partition type is "DAY" which is the default.
        if time_partitioning:
            job_config.time_partitioning = bigquery.table.TimePartitioning(field=partition_field)
        
        return job_config
    
    def load_df(self, df, destination_table, schema, overwrite=True, time_partitioning=False, partition_field=None):
        """
        Loads a pandas dataframe to BigQuery and returns a handler to the job.
        
        Args:
            df: pandas dataframe
            destination_table: the table that the job will write to (e.g. 'project_id.dataset_id.table_id') (for time partition tables, 'project_id.dataset_id.table_id$YYYYMMDD')
            schema: a list of tuples that contains name and field_type
            overwrite: a boolean where True will overwrite destination table and False will append to it
            time_partitioning: a boolean where True will load a partition table
            partition_field: the column to partition the table on (column can be TIMESTAMP or DATE data type)
        
        Returns:
            google.cloud.bigquery.job.LoadJob: a handler to the load job
        """
        job_config = self.config_job(schema, overwrite, time_partitioning, partition_field)
        load_job = self.client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        self.job_history.append(load_job)
        
        return load_job

class Extractor:
    """
    Extracts BigQuery tables to Google Cloud Storage.
    
    Attributes:
        client: BigQuery client
        run_project: the project that the job will run on behalf of
        job_history: a list of handlers to extract jobs ran
    """
    def __init__(self, client=None, run_project="stanleysfang"):
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(project=run_project)
        
        self.run_project = self.client.project
        self.job_history=[]
    
    def config_job(self, destination_format='CSV', field_delimiter=',', print_header=True):
        job_config = bigquery.ExtractJobConfig()
        
        job_config.destination_format = destination_format
        job_config.field_delimiter = field_delimiter
        job_config.print_header = print_header
        
        return job_config
    
    def extract(self, table, gs_path, destination_format='CSV', field_delimiter=',', print_header=True):
        """
        Extracts a BigQuery table to Google Cloud Storage and returns a handler to the job.
        
        Args:
            table: BigQuery table (e.g. 'project_id.dataset_id.table_id')
            gs_path: Google Cloud Storage path (e.g. 'gs://gs_bucket/table.csv')
            destination_format: exported file format
            field_delimiter: delimiter to use between fields in the exported data
            print_header: a boolean where True will print a header row in the exported data
        
        Returns:
            google.cloud.bigquery.job.ExtractJob: a handler to the extract job
        """
        job_config = self.config_job(destination_format, field_delimiter, print_header)
        extract_job = self.client.extract_table(table, gs_path, job_config=job_config)
        self.job_history.append(extract_job)
        
        return extract_job

class Copier:
    """
    Copies BigQuery tables.
    
    Attributes:
        client: BigQuery client
        run_project: the project that the job will run on behalf of
        job_history: a list of handlers to copy jobs ran
    """
    def __init__(self, client=None, run_project="stanleysfang"):
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(project=run_project)
        
        self.run_project = self.client.project
        self.job_history=[]
    
    def config_job(self, overwrite=True):
        job_config = bigquery.CopyJobConfig()
        
        if overwrite:
            job_config.write_disposition = 'WRITE_TRUNCATE'
        else:
            job_config.write_disposition = 'WRITE_APPEND'
        
        return job_config
    
    def copy(self, source_table, destination_table, overwrite=True):
        """
        Copies a BigQuery table and returns a handler to the job.
        
        Args:
            source_table: the source table to copy from (e.g. 'project_id.dataset_id.table_id')
            destination_table: the destination table to copy to (e.g. 'project_id.dataset_id.table_id') (for time partition tables, 'project_id.dataset_id.table_id$YYYYMMDD')
            overwrite: a boolean where True will overwrite destination table and False will append to it
        
        Returns:
            google.cloud.bigquery.job.CopyJob: a handler to the copy job
        """
        job_config = self.config_job(overwrite)
        copy_job = self.client.copy_table(source_table, destination_table, job_config=job_config)
        self.job_history.append(copy_job)
        
        return copy_job
