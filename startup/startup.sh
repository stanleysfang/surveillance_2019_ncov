#!/bin/bash

#### Environment ####
repo="surveillance_2019_ncov"
repo_path=/home/sfang/windows/gitlab/stanleysfang/${repo}/

#### startup ####
conda remove -y --name ${repo} --all

conda create -y --name ${repo} python=3.7.0 pip
source activate ${repo}
pip install -r ${repo_path}/startup/requirements.txt

jupyter kernelspec remove -f ${repo}
python -m ipykernel install --name ${repo} --display-name "Python 3.7.0 (${repo})" --user
