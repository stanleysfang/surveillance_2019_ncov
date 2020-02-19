#!/bin/bash

#### Environment ####

project="stanleysfang"

repository="surveillance_2019_ncov"

#### startup ####

# Install Anaconda
wget "https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh"
bash $HOME/Anaconda3-5.2.0-Linux-x86_64.sh -b
echo -e "\n# added by Anaconda3 installer\nexport PATH=\"$HOME/anaconda3/bin:\$PATH\"" >> $HOME/.bashrc # this doesn't work with airflow
rm $HOME/Anaconda3-5.2.0-Linux-x86_64.sh
export PATH="$HOME/anaconda3/bin:$PATH"

# Create Conda Environment
conda create -y --name surveillance_2019_ncov python=3.7.0 pip
source activate surveillance_2019_ncov
pip install -r $HOME/${repository}/startup/requirements.txt
