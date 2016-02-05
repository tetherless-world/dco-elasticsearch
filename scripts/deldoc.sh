#!/bin/sh

if [ $# != 2 ]
then
    echo "usage: $0 <type> <id>"
    echo "  where type is one of person, project, publication, dataset, datatype"
    echo "  where id is the second part of the DCO-ID (without 11121/)"
    exit 1
fi

curl -XDELETE http://localhost:9200/dco/$1/$2

