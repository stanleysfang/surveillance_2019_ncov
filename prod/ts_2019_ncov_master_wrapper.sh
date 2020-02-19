#!/bin/bash

start_time=$(date)

#### Environment ####

project="stanleysfang"

gs_bucket="stanleysfang"
repository="surveillance_2019_ncov"

instance_name="worker-1"
zone="us-west1-b"

#### ts_2019_ncov_master_wrapper ####

gcloud compute instances start ${instance_name} --zone ${zone}

command="gsutil -m cp -r gs://${gs_bucket}/${repository} \$HOME/"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="bash \$HOME/${repository}/prod/ts_2019_ncov_wrapper.sh 1>\$HOME/${repository}/log/ts_2019_ncov_wrapper.out 2>&1"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="gsutil -m cp -r \$HOME/${repository}/log gs://${gs_bucket}/${repository}/"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

gcloud compute instances stop ${instance_name} --zone ${zone}

#### Run Time ####
end_time=$(date)
start=$(date -d "${start_time}" +%s)
end=$(date -d "${end_time}" +%s)
secs=$(($end-$start))

echo -e "\n================================"
echo -e "Script: ${0##/*/} $*"
echo -e "\nStart Time: ${start_time}"
echo -e "End Time: ${end_time}"
printf 'Run Time: %d day %d hr %d min %d sec\n' $(($secs/86400)) $(($secs%86400/3600)) $(($secs%3600/60)) $(($secs%60))
echo -e "================================"
