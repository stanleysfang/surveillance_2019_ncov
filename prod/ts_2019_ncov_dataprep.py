
from google.cloud import bigquery
import pandas
import datetime
import re

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

#### Load Data ####

confirmed = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
deaths = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
recovered = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')

dt_cols = [datetime.datetime.strptime(dt, '%m/%d/%y').strftime('dt_%Y%m%d') for dt in list(confirmed.columns)[4:]]

confirmed.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols
deaths.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols
recovered.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols

# Job Config
job_config = bigquery.LoadJobConfig()

job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.schema = [
    bigquery.SchemaField('province_state', 'STRING'),
    bigquery.SchemaField('country_region', 'STRING'),
    bigquery.SchemaField('latitude', 'FLOAT64'),
    bigquery.SchemaField('longitude', 'FLOAT64')    
] + [bigquery.SchemaField(dt_col, 'INT64') for dt_col in dt_cols]

# Load Job
for df, metric in [(confirmed, 'confirmed'), (deaths, 'deaths'), (recovered, 'recovered')]:
    load_job = client.load_table_from_dataframe(
        df,
        destination='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{metric}_raw'.format(metric=metric),
        job_config=job_config
    )
    load_job.result()

#### Dataprep ####

# Query
ts_2019_ncov_temp_query = ''

for dt_col in dt_cols:
    ts_2019_ncov_temp_query = \
    """{ts_2019_ncov_temp_query}
    SELECT
        PARSE_DATE('dt_%Y%m%d', '{dt_col}') AS dt,
        a.province_state, a.country_region, a.latitude, a.longitude,
        a.{dt_col} AS confirmed,
        b.{dt_col} AS deaths,
        c.{dt_col} AS recovered,
    FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_confirmed_raw` a
    LEFT JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_deaths_raw` b
    ON IFNULL(a.province_state, '') = IFNULL(b.province_state, '') AND a.country_region = b.country_region
    LEFT JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_recovered_raw` c
    ON IFNULL(a.province_state, '') = IFNULL(c.province_state, '') AND a.country_region = c.country_region
    
    UNION ALL
    """.format(ts_2019_ncov_temp_query=ts_2019_ncov_temp_query, dt_col=dt_col)

ts_2019_ncov_temp_query = re.sub('[(UNION ALL)\n ]*$', '\n', ts_2019_ncov_temp_query)

ts_2019_ncov_query = \
"""
SELECT
    dt, province_state, country_region, latitude, longitude,
    confirmed, deaths, recovered,
    IFNULL(confirmed_new, confirmed) AS confirmed_new,
    IFNULL(deaths_new, deaths) AS deaths_new,
    IFNULL(recovered_new, recovered) AS recovered_new
FROM (
    SELECT
        dt, province_state, country_region, latitude, longitude,
        confirmed, deaths, recovered,
        confirmed - LAG(confirmed) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS confirmed_new,
        deaths - LAG(deaths) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS deaths_new,
        recovered - LAG(recovered) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS recovered_new
    FROM ({ts_2019_ncov_temp_query})
)
""".format(ts_2019_ncov_temp_query=ts_2019_ncov_temp_query)

# Job Config
job_config = bigquery.QueryJobConfig()

job_config.use_legacy_sql = False
job_config.destination = 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov'
job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.time_partitioning = bigquery.table.TimePartitioning(field='dt')
job_config.dry_run = False

# Query Job
query_job = client.query(ts_2019_ncov_query, job_config=job_config)
query_job.result()

#### Extract Table ####

# Job Config
job_config = bigquery.ExtractJobConfig()

job_config.destination_format = 'CSV'

# Extract Job
extract_job = client.extract_table(
    'stanleysfang.surveillance_2019_ncov.ts_2019_ncov',
    'gs://surveillance_2019_ncov/ts_2019_ncov.csv',
    job_config=job_config
)
extract_job.result()
