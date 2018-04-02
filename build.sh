#!/bin/bash

wget https://dl.google.com/go/go1.10.1.linux-amd64.tar.gz
tar -xzvf go1.10.1.linux-amd64.tar.gz
rm go1.10.linux-amd64.tar.gz

# go build server.go config.go request.go fileHandler.go logging.go
./go/bin/go build server.go config.go request.go fileHandler.go logging.go
