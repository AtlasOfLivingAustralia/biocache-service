#!/usr/bin/env bash

hostFile=/etc/hosts

zkNetworkAddr='172.30.1.'
solrNetworkAddr='172.30.2.'

usage() {
    echo "usage: $0 < test | prod > < up | down >"
    exit
}

addHosts() {

    rmHosts

    i=1
    for host in ${zkNodes[*]}; do

        sudo sh -c "echo ${zkNetworkAddr}${i}   ${host} >> ${hostFile}"
        i=$[i+1]

    done

    i=1
    for host in ${solrNodes[*]}; do

        sudo sh -c "echo ${zkNetworkAddr}${i}   ${host} >> ${hostFile}"
        i=$[i+1]

    done
}

rmHosts() {

    sudo sed -i '' "/${zkNetworkAddr}/d" ${hostFile}
    sudo sed -i '' "/${solrNetworkAddr}/d" ${hostFile}
}

addNetworkInterfaces() {

    i=1
    for host in ${zkNodes[*]}; do

        sudo ifconfig lo0 alias ${zkNetworkAddr}${i} up
        i=$[i+1]

    done

    i=1
    for host in ${solrNodes[*]}; do

        sudo ifconfig lo0 alias ${solrNetworkAddr}${i} up
        i=$[i+1]

    done
}

removeNetworkInterfaces() {

    i=1
    for host in ${zkNodes[*]}; do

        sudo ifconfig lo0 remove ${zkNetworkAddr}${i} up
        i=$[i+1]

    done

    i=1
    for host in ${solrNodes[*]}; do

        sudo ifconfig lo0 remove ${solrNetworkAddr}${i} up
        i=$[i+1]

    done
}

setupSshTunnels() {

    i=1
    for host in ${zkNodes[*]}; do
        echo ssh -f -N ${host} -L ${zkNetworkAddr}${i}:2181:localhost:2181
        ssh -f -N ${host} -L ${zkNetworkAddr}${i}:2181:localhost:2181
        i=$[i+1]

    done

    i=1
    for host in ${solrNodes[*]}; do
        echo ssh -f -N ${host} -L ${solrNetworkAddr}${i}:8983:localhost:8983
        ssh -f -N ${host} -L ${solrNetworkAddr}${i}:8983:localhost:8983
        i=$[i+1]

    done
}

[ $# -ne 2 ] && usage

case $1 in
    'test')
        zkNodes=('nci3-zookeeper-1.ala' 'nci3-zookeeper-2.ala' 'nci3-zookeeper-3.ala')
        solrNodes=('nci3-solr-1.ala' 'nci3-solr-2.ala' 'nci3-solr-3.ala' 'nci3-solr-4.ala')
    ;;
    'prod')
        zkNodes=('aws-zoo-2021-1.ala' 'aws-zoo-2021-2.ala' 'aws-zoo-2021-3.ala' 'aws-zoo-2021-4.ala' 'aws-zoo-2021-5.ala')
        solrNodes=('aws-solr-2021-1.ala' 'aws-solr-2021-2.ala' 'aws-solr-2021-3.ala' 'aws-solr-2021-4.ala' 'aws-solr-2021-5.ala' 'aws-solr-2021-6.ala' 'aws-solr-2021-7.ala' 'aws-solr-2021-8.ala')
    ;;
    *)
    usage
esac

case $2 in
    'up')
        echo 'up' ${zkNodes[*]} ${solrNodes[*]}
        addHosts
        addNetworkInterfaces
        setupSshTunnels
    ;;
    'down')
        echo 'down' ${zkNodes[*]} ${solrNodes[*]}

        removeNetworkInterfaces
        rmHosts
        pkill ssh
    ;;
    *) usage
esac
