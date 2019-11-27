#!/bin/bash
SCRIPT_PATH=`dirname $(readlink -f $0)`
cd $SCRIPT_PATH

# Deploy a testing cluster with one master and 3 workers
docker-compose up --scale ns_worker=3 -d 

# Since we can't resolved domains yet, we have to get each container IP to create the config file
WORKER_IPS=$(docker-compose ps | grep -oE "docker_ns_worker_[0-9]+" \
| xargs -I{} docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' {})
read -a WORKER_IPS <<< $(echo $WORKER_IPS)
slaves=$(printf ",\"ns_user@%s\"" "${WORKER_IPS[@]}")
slaves="slaves = [${slaves:1}]"

MASTER_IP=$(docker-compose ps | grep -oE "docker_ns_master_[0-9]+" \
| xargs -I{} docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' {})

CONF_FILE=`cat <<EOF
master = "$MASTER_IP:3000"
${slaves}
EOF
`

count=0
for WORKER in $(docker-compose ps | grep -oE "docker_ns_worker_[0-9]+")
do
    echo "Setting $WORKER";
    docker exec -e CONF_FILE="$CONF_FILE" -e NS_LOCAL_IP="${WORKER_IPS[count]}" -w /home/ns_user/ $WORKER \
    bash -c 'echo "$CONF_FILE" >> hosts.conf && \
    echo "NS_LOCAL_IP=$NS_LOCAL_IP" >> .ssh/environment && \
    echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config && \
    service ssh start';
    (( count++ ));
done

docker exec -e CONF_FILE="$CONF_FILE" -e NS_LOCAL_IP="${MASTER_IP}" -w /root/ docker_ns_master_1 \
    bash -c 'echo "$CONF_FILE" >> hosts.conf && echo "export NS_LOCAL_IP=$NS_LOCAL_IP" >> .bashrc'
for WORKER_IP in ${WORKER_IPS[@]}
do
    docker exec docker_ns_master_1 bash -c "ssh-keyscan ${WORKER_IP} >> ~/.ssh/known_hosts"
done

# When done is posible to open a shell into the master and run any of the examples in distributed mode
