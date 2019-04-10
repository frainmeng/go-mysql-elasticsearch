#!/bin/bash
# it's very important !!!
source /etc/profile

program_name=go-mysql-postgresql-mw

rm -f bin/${program_name}

GO111MODULE=on go build -o bin/${program_name} ./cmd/go-mysql-elasticsearch
