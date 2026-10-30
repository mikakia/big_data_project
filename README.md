# 1. Cluster Setup
The docker compose file contains volumes so that each cluster has its own data folder. It also creates a history server that shows event logs that can be viewed at http://localhost:18080

## Scripts, read only
./src:/opt/spark/work-dir/src:ro 
## Shared path Spark reads from, allows the recombination of data
./data:/opt/spark/work-dir/data 
## Each nodes individual data part
./data/master:/data

## Start the cluster
docker compose up -d
## Check what is running
docker ps

# 2. Manual Data Splitting accross Nodes
The manual split is done in 'split_data.py' where the data were splitted into 3 different csv files. The volumes give each node access to its own individual dataset.

### command explanation
docker exec runs a command inside a running container.

### To check if the folders were successfully created
docker exec <container name> ls -ld /data   

### To check if the files were successfully copied
docker exec <container name> ls -l /data

# 3. Data loading on each node
The data loading is done in 'load_data.py'

docker exec -it galaxy-master /opt/spark/bin/spark-submit --master spark://galaxy-master:7077 /opt/spark/work-dir/src/load_data.py