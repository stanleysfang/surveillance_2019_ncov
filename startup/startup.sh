#!/bin/bash

#### Initialization ####

start_time=$(date)

echo -e "\n================================\nRunning         ${0##/*/} $*\n"

#### Startup ####

repository="surveillance_2019_ncov"

# Install Anaconda
wget "https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh"
bash $HOME/Anaconda3-5.2.0-Linux-x86_64.sh -b
echo -e "\n# added by Anaconda3 installer\nexport PATH=\"$HOME/anaconda3/bin:\$PATH\"" >> $HOME/.bashrc # this doesn't work with airflow
rm $HOME/Anaconda3-5.2.0-Linux-x86_64.sh
export PATH="$HOME/anaconda3/bin:$PATH"

# Create Conda Environments
# surveillance_2019_ncov env
conda create -y --name surveillance_2019_ncov python=3.7.0 pip
source activate surveillance_2019_ncov
pip install -r $HOME/${repository}/production/requirements.txt

#### Run Time ####

end_time=$(date)
start=$(date -d "${start_time}" +%s)
end=$(date -d "${end_time}" +%s)
secs=$(($end-$start))

echo -e "\nScript          ${0##/*/} $*\nStart Time      ${start_time}\nEnd Time        ${end_time}"
printf 'Run Time        %d day %d hr %d min %d sec\n================================\n' $(($secs/86400)) $(($secs%86400/3600)) $(($secs%3600/60)) $(($secs%60))
