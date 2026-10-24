# Start cluster
docker start galaxy-master galaxy-worker1 galaxy-worker2
# Check what is running 
docker ps

# 2. Manual Data Splitting accross Nodes
## Create a data folder on each node
docker exec -u root galaxy-master mkdir -p /data
docker exec -u root galaxy-worker1 mkdir -p /data
docker exec -u root galaxy-worker2 mkdir -p /data

### command explanation
docker exec runs a command inside a running container.
-u root runs it as the root user, which is needed to create a folder at /.
mkdir -p /data creates the /data folder if they don't already exist(-p)

### To check if the folders were successfully created
docker exec <container name> ls -ld /data   

## Copy the data to each node (from VS Code terminal while you are in the project folder)
docker cp data/master/master.csv galaxy-master:/data/master.csv
docker cp data/worker1/worker1.csv galaxy-worker1:/data/worker1.csv
docker cp data/worker2/worker2.csv galaxy-worker2:/data/worker2.csv

### To check if the files were successfully copied
docker exec <container name> ls -l /data

