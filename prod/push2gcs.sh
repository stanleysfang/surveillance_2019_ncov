#!/bin/bash

start_time=$(date)

#### Environment ####

project_id="stanleysfang"

gs_bucket="stanleysfang"
repo="surveillance_2019_ncov"

code_path="/home/sfang/windows/gitlab/stanleysfang/${repo}/"

instance_name="stanleysfang"
zone="us-west1-b"
home_path="/home/stanleysfang92/" # need this home_path because $HOME can be a different user's home directory

#### push2gcs ####

gsutil -m rsync -dr -x "__pycache__" ${code_path}prod gs://${gs_bucket}/${repo}/prod
gsutil -m rsync -dr ${code_path}startup gs://${gs_bucket}/${repo}/startup

command="sudo gsutil -m rsync -dr gs://${gs_bucket}/${repo} ${home_path}${repo}"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="sudo chmod 755 ${home_path}${repo}/prod/*"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="sudo chmod 666 ${home_path}${repo}/startup/*"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

command="sudo chmod 666 ${home_path}${repo}/log/*"
gcloud compute ssh ${instance_name} --zone ${zone} --command "${command}"

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
