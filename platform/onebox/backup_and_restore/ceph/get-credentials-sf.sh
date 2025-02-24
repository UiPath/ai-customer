#!/bin/bash

: '
This scipt will generate json file for creds to be used with import export
Script will generate file storage-creds-sf.json
Run it from the VM running aifabric.
Use it as it is [insecure] or transfer to some credsManager and then change backup/restore scripts to fetch from credsmanager instead of json file# $1 - [Optional but recommended] pass private ip of the aif machine on which it is accesible from other vms in the same network
[Script Version -> 21.4]
'

readonly PUBLIC_IP=$1

function initialize_variables() {
        if [ -z "$PUBLIC_IP" ]; then
        ## If public ip not given as an argument.
        OBJECT_GATEWAY_EXTERNAL_HOST=$(kubectl -n istio-system get vs cephobjectstore-vs -o json | jq '.spec.hosts[0]' | tr -d '"')
    else
        OBJECT_GATEWAY_EXTERNAL_HOST=$PUBLIC_IP
    fi
    echo "$green $(date) Public IP was $PUBLIC_IP and OBJECT_GATEWAY_EXTERNAL_HOST is $OBJECT_GATEWAY_EXTERNAL_HOST"

        STORAGE_ACCESS_KEY=$(kubectl -n uipath get secret deployment-storage-credentials -o json | jq '.data.".dockerconfigjson"' | sed -e 's/^"//' -e 's/"$//' | base64 -d | jq '.access_key' | sed -e 's/^"//' -e 's/"$//')
        STORAGE_SECRET_KEY=$(kubectl -n uipath get secret deployment-storage-credentials -o json | jq '.data.".dockerconfigjson"' | sed -e 's/^"//' -e 's/"$//' | base64 -d | jq '.secret_key' | sed -e 's/^"//' -e 's/"$//')

        readonly AWS_HOST=$OBJECT_GATEWAY_EXTERNAL_HOST
        readonly AWS_ENDPOINT="https://${OBJECT_GATEWAY_EXTERNAL_HOST}"
        readonly AWS_ACCESS_KEY_ID=$STORAGE_ACCESS_KEY
        readonly AWS_SECRET_ACCESS_KEY=$STORAGE_SECRET_KEY
}

function generate_json() {
        echo '{"AWS_HOST": "'$AWS_HOST'", "AWS_ENDPOINT": "'$AWS_ENDPOINT'", "AWS_ACCESS_KEY_ID": "'$AWS_ACCESS_KEY_ID'", "AWS_SECRET_ACCESS_KEY": "'$AWS_SECRET_ACCESS_KEY'"}' > storage-creds-sf.json
}

initialize_variables
generate_json