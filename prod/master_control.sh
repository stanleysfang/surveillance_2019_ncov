#!/bin/bash

start_time=$(date)

#### Environment ####

project="stanleysfang"

gs_bucket="stanleysfang"
repository="surveillance_2019_ncov"
home_path="/home/stanleysfang92/"

instance_name="stanleysfang"
zone="us-west1-b"

#### master_control ####

# gcloud compute instances start ${instance_name} --zone ${zone}

command="sudo gsutil -m cp -r gs://${gs_bucket}/${repository} ${home_path}"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

# command="sudo rm ${home_path}${repository}/log/*"
# gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="sudo bash ${home_path}${repository}/prod/ts_2019_ncov_master_wrapper.sh" # do not have permission to ${home_path} 1>${home_path}${repository}/log/ts_2019_ncov_master_wrapper.out 2>&1
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

# command="sudo gsutil -m cp -r ${home_path}${repository}/log gs://${gs_bucket}/${repository}/"
# gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

# gcloud compute instances stop ${instance_name} --zone ${zone}

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
