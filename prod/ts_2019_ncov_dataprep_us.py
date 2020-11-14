
from google.cloud import bigquery
from BigQueryWrapper import QueryRunner, Loader, Extractor
import pandas as pd
import datetime
import re

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 500)

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

qr = QueryRunner(client=client)
loader = Loader(client=client)
extractor = Extractor(client=client)

geo='us'

#### Functions ####
def print_job_result(job, client, max_results=20):
    job.result()
    bq_table = client.get_table(job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    print(bq_table.full_table_id)
    print(df.head(max_results))

#### Load Data ####
confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')
deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv')

dt_cols = [datetime.datetime.strptime(dt, '%m/%d/%y').strftime('dt_%Y%m%d') for dt in list(confirmed.columns)[11:]]

# confirmed
confirmed_cols = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'county', 'province_state', 'country_region', 'latitude', 'longitude', 'combined_key'] + dt_cols
confirmed.columns = confirmed_cols

confirmed_schema = [
    ('UID', 'INT64'),
    ('iso2', 'STRING'),
    ('iso3', 'STRING'),
    ('code3', 'INT64'),
    ('FIPS', 'FLOAT64'),
    ('county', 'STRING'),
    ('province_state', 'STRING'),
    ('country_region', 'STRING'),
    ('latitude', 'FLOAT64'),
    ('longitude', 'FLOAT64'),
    ('combined_key', 'STRING'),
] + [(dt_col, 'INT64') for dt_col in dt_cols]

confirmed_load_job = loader.load_df(confirmed, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed_raw'.format(geo=geo), schema=confirmed_schema)

#deaths
deaths_cols = confirmed_cols
deaths_cols.insert(11, 'population')
deaths.columns = deaths_cols

deaths_schema = confirmed_schema
deaths_schema.insert(11, ('population', 'INT64'))

deaths_load_job = loader.load_df(deaths, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths_raw'.format(geo=geo), schema=deaths_schema)

for job in loader.job_history:
    job.result()
    
    bq_table = client.get_table(job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    
    print(bq_table.full_table_id)
    print(df.head(max_results))

#### Dataprep ####
for metric in ['confirmed', 'deaths']:
    array_query = ''
    for dt_col in dt_cols:
        array_query = \
        """{array_query}
        STRUCT(PARSE_DATE('dt_%Y%m%d', '{dt_col}') AS dt, {dt_col} AS {metric}),
        """.format(array_query=array_query, dt_col=dt_col, metric=metric)
    array_query = re.sub('[,\n ]*$', '\n', array_query)
    
    query = \
    """
    SELECT dt, a.* EXCEPT(arr), {metric}
    FROM (
        SELECT
            county, province_state, country_region, combined_key, latitude, longitude,
            [{array_query}] AS arr
        FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_{metric}_raw`
    ) a, UNNEST(arr)
    """.format(metric=metric, array_query=array_query, geo=geo)
    
    query_job = qr.run_query(query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_{metric}'.format(geo=geo, metric=metric), time_partitioning=True, partition_field='dt')
    query_job.result()
    
    bq_table = client.get_table(query_job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    
    print(bq_table.full_table_id)
    print(df.head(max_results))

ts_2019_ncov_query = \
"""
SELECT
    dt, county, province_state, country_region, combined_key, latitude, longitude,
    population,
    confirmed, deaths, confirmed_new, deaths_new,
    ROUND(AVG(confirmed_new) OVER(PARTITION BY combined_key ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS confirmed_new_7d_ma,
    ROUND(AVG(deaths_new) OVER(PARTITION BY combined_key ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS deaths_new_7d_ma,
    ROUND(AVG(confirmed_new) OVER(PARTITION BY combined_key ORDER BY dt ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 1) AS confirmed_new_28d_ma,
    ROUND(AVG(deaths_new) OVER(PARTITION BY combined_key ORDER BY dt ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 1) AS deaths_new_28d_ma,
    IF(population = 0, NULL, ROUND(confirmed/population, 4)) AS incident_rate,
    IF(confirmed = 0, NULL, ROUND(deaths/confirmed, 4)) AS case_fatality_rate,
    MAX(dt) OVER() AS last_update_dt,
    TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS last_updated_ts
FROM (
    SELECT
        * EXCEPT(confirmed_new, deaths_new),
        IFNULL(confirmed_new, confirmed) AS confirmed_new,
        IFNULL(deaths_new, deaths) AS deaths_new
    FROM (
        SELECT
            *,
            confirmed - LAG(confirmed) OVER(PARTITION BY combined_key ORDER BY dt) AS confirmed_new,
            deaths - LAG(deaths) OVER(PARTITION BY combined_key ORDER BY dt) AS deaths_new
        FROM (
            SELECT
                a.*, b.population
            FROM (
                SELECT
                    COALESCE(a.dt, b.dt) AS dt,
                    COALESCE(a.county, b.county) AS county,
                    COALESCE(a.province_state, b.province_state) AS province_state,
                    COALESCE(a.country_region, b.country_region) AS country_region,
                    COALESCE(a.combined_key, b.combined_key) AS combined_key,
                    COALESCE(a.latitude, b.latitude) AS latitude,
                    COALESCE(a.longitude, b.longitude) AS longitude,
                    IFNULL(a.confirmed, 0) AS confirmed,
                    IFNULL(b.deaths, 0) AS deaths
                FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed` a
                FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths` b
                ON a.dt = b.dt AND a.combined_key = b.combined_key
            ) a
            LEFT JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths_raw` b
            ON a.combined_key = b.combined_key
        )
    )
)
""".format(geo=geo)

query_job = qr.run_query(ts_2019_ncov_query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}'.format(geo=geo), time_partitioning=True, partition_field='dt')
query_job.result()

bq_table = client.get_table(query_job.destination)
df = client.list_rows(bq_table, max_results=max_results).to_dataframe()

print(bq_table.full_table_id)
print(df.head(max_results))

us_cur_query = \
"""
SELECT *
FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}`
WHERE dt = last_update_dt
""".format(geo=geo)

#### Extract Table ####
for dt_col in dt_cols[-7:]: #  only refresh the last 14 days
    dt = datetime.datetime.strptime(dt_col, 'dt_%Y%m%d').strftime('%Y-%m-%d')
    print(dt)
    
    daily_ts_2019_ncov_query = \
    """
    SELECT *
    FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}`
    WHERE dt = "{dt}"
    """.format(geo=geo, dt=dt)
    
    query_job = qr.run_query(daily_ts_2019_ncov_query)
    query_job.result()
    
    extract_job = extractor.extract(query_job.destination, 'gs://surveillance_2019_ncov/ts_2019_ncov_{geo}_{dt}.csv'.format(geo=geo, dt=re.sub('-', '', dt)))
    extract_job.result()
