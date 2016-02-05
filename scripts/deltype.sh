#!/bin/sh

if [ $# != 1 ]
then
    echo "usage: $0 <type>"
    echo "  where type is one of person, project, publication, dataset, datatype"
    exit 1
fi

curl -XDELETE http://localhost:9200/dco/$1

