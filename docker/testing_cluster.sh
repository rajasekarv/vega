#!/bin/bash
SCRIPT_PATH=`dirname $(readlink -f $0)`
cd $SCRIPT_PATH

# Deploy a testing cluster with one master and 3 workers
docker-compose up --scale ns_worker=3 -d 

# Since we can't resolved domains yet, we have to get each container IP to create the config file
WORKER_IPS=$(docker-compose ps | grep -oE "docker_ns_worker_[0-9]+" \
| xargs -I{} docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' {})
read -a WORKER_IPS <<< $(echo $WORKER_IPS)
slaves=$(printf ",\"user@%s\"" "${WORKER_IPS[@]}")
slaves="slaves = [${slaves:1}]"

MASTER_IP=$(docker-compose ps | grep -oE "docker_ns_master_[0-9]+" \
| xargs -I{} docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' {})

CONF_FILE=`cat <<EOF
master = "$MASTER_IP:3000"
${slaves}
EOF
`

for worker in $(docker-compose ps | grep -oE "docker_ns_worker_[0-9]+")
do
    echo "Setting $worker"
    docker exec -e CONF_FILE="$CONF_FILE" -w /root/ $worker bash -c 'echo "$CONF_FILE" >> hosts.conf'
done

docker exec -e CONF_FILE="$CONF_FILE" -w /root/ docker_ns_master_1 bash -c 'echo "$CONF_FILE" >> hosts.conf'

# When done you can shell into the master and run any of examples in remote mode
