
from google.cloud import bigquery
from BigQueryWrapper import QueryRunner, Loader, Extractor
import pandas as pd
import datetime
import re

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

qr = QueryRunner(client=client)
loader = Loader(client=client)
extractor = Extractor(client=client)

max_results = 20

#### Load Data ####
confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
recovered = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')

dt_cols = [datetime.datetime.strptime(dt, '%m/%d/%y').strftime('dt_%Y%m%d') for dt in list(confirmed.columns)[4:]]

confirmed.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols
deaths.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols
recovered.columns = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols

schema = [
    ('province_state', 'STRING'),
    ('country_region', 'STRING'),
    ('latitude', 'FLOAT64'),
    ('longitude', 'FLOAT64')    
] + [(dt_col, 'INT64') for dt_col in dt_cols]

for df, metric in [(confirmed, 'confirmed'), (deaths, 'deaths'), (recovered, 'recovered')]:
    load_job = loader.load_df(df, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{metric}_raw'.format(metric=metric), schema=schema)
    load_job.result()
    
    bq_table = client.get_table(load_job.destination)
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
        COALESCE(a.province_state, b.province_state, c.province_state) AS province_state,
        COALESCE(a.country_region, b.country_region, c.country_region) AS country_region,
        COALESCE(a.latitude, b.latitude, c.latitude) AS latitude,
        COALESCE(a.longitude, b.longitude, c.longitude) AS longitude,
        IFNULL(a.{dt_col}, 0) AS confirmed,
        IFNULL(b.{dt_col}, 0) AS deaths,
        IFNULL(c.{dt_col}, 0) AS recovered,
    FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_confirmed_raw` a
    FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_deaths_raw` b
    ON IFNULL(a.province_state, '') = IFNULL(b.province_state, '') AND a.country_region = b.country_region
    FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_recovered_raw` c
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
    IFNULL(recovered_new, recovered) AS recovered_new,
    TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{{2}}$', '')) AS last_updated_ts
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

query_job = qr.run_query(ts_2019_ncov_query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov', time_partitioning=True, partition_field='dt')
query_job.result()

bq_table = client.get_table(query_job.destination)
df = client.list_rows(bq_table, max_results=max_results).to_dataframe()

print(bq_table.full_table_id)
print(df.head(max_results))

#### Extract Table ####
extract_job = extractor.extract('stanleysfang.surveillance_2019_ncov.ts_2019_ncov', 'gs://surveillance_2019_ncov/ts_2019_ncov.csv')
extract_job.result()
