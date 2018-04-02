#!/bin/bash


if [ ! -d "go" ]; then
    wget https://dl.google.com/go/go1.10.1.linux-amd64.tar.gz
    tar -xzvf go1.10.1.linux-amd64.tar.gz
    rm go1.10.1.linux-amd64.tar.gz
fi

# go build server.go config.go request.go fileHandler.go logging.go
./go/bin/go build server.go config.go request.go fileHandler.go logging.go
