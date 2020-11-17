# Surveillance of Coronavirus COVID-2019
The data source is produced by John Hopkins University Center for Systems Science and Engineering ([JHU CSSE](https://github.com/CSSEGISandData/COVID-19)) copyright 2020.

This repository takes the data collected by JHU CSSE and further processes it to supplement the dashboard done by the university. It adds:
- Searchable US county level stats in tabular form

**Dashboard:** https://public.tableau.com/profile/stanleysfang#!/vizhome/COVID-2019Surveillance/CurrentUSReport-CountyLevel  
**JHU CSSE Dashboard:** https://arcg.is/0fHmTX

### Data Pipeline
Tableau Public has limited options for connecting to live data, and Google Spreadsheet is one of the free tools that can support a live dashboard. Therefore, the final destination of the pipeline will be at Google Spreadsheet. Figure-1 is a diagram that describes the pipeline.

<img src="https://github.com/stanleysfang/surveillance_2019_ncov/raw/master/image/pipeline_diagram.png" alt="pipeline_diagram" width="730" height="300">

*Figure-1: The arrows show the flow of data. Compute Engine and Cloud Functions*  
*support the transfer of data. Data processing is done in BigQuery.*

### References
**Data Source:** https://github.com/CSSEGISandData/COVID-19  
**Information on Coronavirus COVID-2019:** https://www.who.int/emergencies/diseases/novel-coronavirus-2019
