
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

geo='global'

#### Functions ####
def print_job_result(job, client, max_results=20):
    job.result()
    bq_table = client.get_table(job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    print(bq_table.full_table_id)
    print(df.head(max_results))

#### Load Data ####
confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
recovered = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')

dt_cols = [datetime.datetime.strptime(dt, '%m/%d/%y').strftime('dt_%Y%m%d') for dt in list(confirmed.columns)[4:]]
cols = ['province_state', 'country_region', 'latitude', 'longitude'] + dt_cols

confirmed.columns = cols
deaths.columns = cols
recovered.columns = cols

schema = [
    ('province_state', 'STRING'),
    ('country_region', 'STRING'),
    ('latitude', 'FLOAT64'),
    ('longitude', 'FLOAT64')
] + [(dt_col, 'INT64') for dt_col in dt_cols]

loader.load_df(confirmed, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed_raw'.format(geo=geo), schema=schema)
loader.load_df(deaths, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths_raw'.format(geo=geo), schema=schema)
loader.load_df(recovered, 'stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_recovered_raw'.format(geo=geo), schema=schema)

for job in loader.job_history:
    print_job_result(job, client)

#### Dataprep ####
# restructure raw data
for metric in ['confirmed', 'deaths', 'recovered']:
    array_query = ''
    for dt_col in dt_cols:
        array_query = \
        """{array_query}
        STRUCT(PARSE_DATE('dt_%Y%m%d', '{dt_col}') AS dt, {dt_col} AS total_{metric}),
        """.format(array_query=array_query, dt_col=dt_col, metric=metric)
    array_query = re.sub('[,\n ]*$', '\n', array_query)
    
    query = \
    """
    SELECT dt, a.* EXCEPT(arr), total_{metric}
    FROM (
        SELECT
            province_state, country_region, latitude, longitude,
            [{array_query}] AS arr
        FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_{metric}_raw`
    ) a, UNNEST(arr)
    """.format(metric=metric, array_query=array_query, geo=geo)
    
    query_job = qr.run_query(query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_{metric}'.format(geo=geo, metric=metric), time_partitioning=True, partition_field='dt')

for job in qr.job_history:
    print_job_result(job, client)

# agg data
ts_2019_ncov_query = \
"""
WITH
us_province_state_lvl AS (
SELECT
    dt, province_state, country_region,
    SUM(population) AS population,
    total_confirmed, total_
FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_us`
)

SELECT
    dt, province_state, country_region, latitude, longitude,
    total_confirmed, total_deaths, total_recovered, daily_new_confirmed, daily_new_deaths, daily_new_recovered,
    ROUND(AVG(daily_new_confirmed) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS daily_new_confirmed_7d_ma,
    ROUND(AVG(daily_new_deaths) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS daily_new_deaths_7d_ma,
    ROUND(AVG(daily_new_recovered) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS daily_new_recovered_7d_ma,
    ROUND(AVG(daily_new_confirmed) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 1) AS daily_new_confirmed_28d_ma,
    ROUND(AVG(daily_new_deaths) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 1) AS daily_new_deaths_28d_ma,
    ROUND(AVG(daily_new_recovered) OVER(PARTITION BY province_state, country_region ORDER BY dt ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 1) AS daily_new_recovered_28d_ma,
    IF(population = 0, NULL, ROUND(total_confirmed/population, 4)) AS incident_rate,
    IF(total_confirmed = 0, NULL, ROUND(total_deaths/total_confirmed, 4)) AS case_fatality_rate,
    MAX(dt) OVER() AS last_updated_dt,
    TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{{2}}$', '')) AS last_updated_ts -- need the double bracket to avoid error with str.format
FROM (
    SELECT
        * EXCEPT(daily_new_confirmed, daily_new_deaths, daily_new_recovered),
        IFNULL(daily_new_confirmed, total_confirmed) AS daily_new_confirmed,
        IFNULL(daily_new_deaths, total_deaths) AS daily_new_deaths,
        IFNULL(daily_new_recovered, total_recovered) AS daily_new_recovered
    FROM (
        SELECT
            *,
            total_confirmed - LAG(total_confirmed) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS daily_new_confirmed,
            total_deaths - LAG(total_deaths) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS daily_new_deaths,
            total_recovered - LAG(total_recovered) OVER(PARTITION BY province_state, country_region ORDER BY dt) AS daily_new_recovered
        FROM (
            SELECT
                COALESCE(a.dt, b.dt, c.dt) AS dt,
                COALESCE(a.province_state, b.province_state, c.province_state) AS province_state,
                COALESCE(a.country_region, b.country_region, c.country_region) AS country_region,
                COALESCE(a.latitude, b.latitude, c.latitude) AS latitude,
                COALESCE(a.longitude, b.longitude, c.longitude) AS longitude,
                IFNULL(a.total_confirmed, 0) AS total_confirmed,
                IFNULL(b.total_deaths, 0) AS total_deaths,
                IFNULL(c.total_recovered, 0) AS total_recovered
            FROM `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_confirmed` a
            FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_deaths` b
            ON a.dt = b.dt AND IFNULL(a.province_state, '') = IFNULL(b.province_state, '') AND a.country_region = b.country_region
            FULL JOIN `stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}_recovered` c -- Canada is not at province_state level in recovered table
            ON a.dt = c.dt AND IFNULL(a.province_state, '') = IFNULL(c.province_state, '') AND a.country_region = c.country_region
        )
    )
)
""".format(geo=geo)


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

query_job = qr.run_query(ts_2019_ncov_query, destination_table='stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}'.format(geo=geo), time_partitioning=True, partition_field='dt')
query_job.result()

bq_table = client.get_table(query_job.destination)
df = client.list_rows(bq_table, max_results=max_results).to_dataframe()

print(bq_table.full_table_id)
print(df.head(max_results))

#### Extract Table ####
extract_job = extractor.extract('stanleysfang.surveillance_2019_ncov.ts_2019_ncov_{geo}'.format(geo=geo), 'gs://surveillance_2019_ncov/ts_2019_ncov_{geo}.csv'.format(geo=geo))
extract_job.result()
