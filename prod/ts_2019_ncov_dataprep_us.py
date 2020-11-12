
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

max_results = 20

geo='us'

#### Load Data ####
confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')
deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv')

dt_cols = [datetime.datetime.strptime(dt, '%m/%d/%y').strftime('dt_%Y%m%d') for dt in list(confirmed.columns)[11:]]
cols = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'city', 'province_state', 'country_region', 'latitude', 'longitude', 'combined_key'] + dt_cols

confirmed.columns = cols

cols.insert(11, 'population')
deaths.columns = cols

confirmed_schema = [
    ('UID', 'INT64'),
    ('iso2', 'STRING'),
    ('iso3', 'STRING'),
    ('code3', 'INT64'),
    ('FIPS', 'FLOAT64'),
    ('city', 'STRING'),
    ('province_state', 'STRING'),
    ('country_region', 'STRING'),
    ('latitude', 'FLOAT64'),
    ('longitude', 'FLOAT64'),
    ('combined_key', 'STRING'),
] + [(dt_col, 'INT64') for dt_col in dt_cols]

confirmed_load_job = loader.load_df(confirmed, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed_raw'.format(geo=geo), schema=schema)

schema.insert(11, ('population', 'INT64'))
deaths_load_job = loader.load_df(deaths, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths_raw'.format(geo=geo), schema=schema)

for job in loader.job_history:
    job.result()
    
    bq_table = client.get_table(job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    
    print(bq_table.full_table_id)
    print(df.head(max_results))

#### Dataprep ####
ts_2019_ncov_temp_query = ''
for dt_col in dt_cols:
    ts_2019_ncov_temp_query = \
    """{ts_2019_ncov_temp_query}
    SELECT
        PARSE_DATE('dt_%Y%m%d', '{dt_col}') AS dt,
        COALESCE(a.city, b.city) AS city,
        COALESCE(a.province_state, b.province_state) AS province_state,
        COALESCE(a.country_region, b.country_region) AS country_region,
        COALESCE(a.combined_key, b.combined_key) AS combined_key,
        COALESCE(a.latitude, b.latitude) AS latitude,
        COALESCE(a.longitude, b.longitude) AS longitude,
        IFNULL(a.{dt_col}, 0) AS confirmed,
        IFNULL(b.{dt_col}, 0) AS deaths,
        0 AS recovered
    FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed_raw` a
    FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths_raw` b
    ON a.combined_key = b.combined_key
    
    UNION ALL
    """.format(ts_2019_ncov_temp_query=ts_2019_ncov_temp_query, dt_col=dt_col, geo=geo)

ts_2019_ncov_temp_query = re.sub('[(UNION ALL)\n ]*$', '\n', ts_2019_ncov_temp_query)

ts_2019_ncov_query = \
"""
SELECT
    dt, province_state, country_region, latitude, longitude,
    confirmed, deaths, recovered,
    IFNULL(confirmed_new, confirmed) AS confirmed_new,
    IFNULL(deaths_new, deaths) AS deaths_new,
    recovered_new,
    TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{{2}}$', '')) AS last_updated_ts
FROM (
    SELECT
        dt,
        CASE
            WHEN city IS NULL OR city = 'Unassigned' THEN province_state
            ELSE CONCAT(city, ", ", province_state)
            END AS province_state,
        country_region, latitude, longitude,
        confirmed, deaths, recovered,
        confirmed - LAG(confirmed) OVER(PARTITION BY combined_key ORDER BY dt) AS confirmed_new,
        deaths - LAG(deaths) OVER(PARTITION BY combined_key ORDER BY dt) AS deaths_new,
        0 AS recovered_new
    FROM ({ts_2019_ncov_temp_query})
)
""".format(ts_2019_ncov_temp_query=ts_2019_ncov_temp_query)

query_job = qr.run_query(ts_2019_ncov_query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}'.format(geo=geo), time_partitioning=True, partition_field='dt')
query_job.result()

bq_table = client.get_table(query_job.destination)
df = client.list_rows(bq_table, max_results=max_results).to_dataframe()

print(bq_table.full_table_id)
print(df.head(max_results))

#### Extract Table ####
for dt_col in dt_cols[-7:]: #  only refresh the last 7 days
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
