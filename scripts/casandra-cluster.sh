#!/usr/bin/env bash

hostFile=/etc/hosts

casNetworkAddr='172.30.3.'

usage() {
    echo "usage: $0 < test | prod > < up | down >"
    exit
}

addHosts() {

    rmHosts

    i=1
    for host in ${casNodes[*]}; do

        sudo sh -c "echo ${casNetworkAddr}${i}   ${host} >> ${hostFile}"
        i=$[i+1]

    done
}

rmHosts() {

    sudo sed -i '' "/${casNetworkAddr}/d" ${hostFile}
}

addNetworkInterfaces() {

    i=1
    for host in ${casNodes[*]}; do

        sudo ifconfig lo0 alias ${casNetworkAddr}${i} up
        i=$[i+1]

    done
}

removeNetworkInterfaces() {

    i=1
    for host in ${casNodes[*]}; do

        sudo ifconfig lo0 remove ${casNetworkAddr}${i} up
        i=$[i+1]

    done
}

setupSshTunnels() {

    i=1
    for host in ${casNodes[*]}; do
        echo ssh -f -N ${host} -L ${casNetworkAddr}${i}:9042:localhost:9042
        ssh -f -N ${host} -L ${casNetworkAddr}${i}:9042:localhost:9042
        i=$[i+1]

    done
}

[ $# -ne 2 ] && usage

case $1 in
    'prod')
        casNodes=('aws-cass-2021-1.ala' 'aws-cass-2021-2.ala')
    ;;
    *)
    usage
esac

case $2 in
    'up')
        echo 'up' ${casNodes[*]}
        addHosts
        addNetworkInterfaces
        setupSshTunnels
    ;;
    'down')
        echo 'down' ${casNodes[*]}

        removeNetworkInterfaces
        rmHosts
        pkill ssh
    ;;
    *) usage
esac
