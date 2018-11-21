#!/bin/bash

#For help installing gogoprotobufs see: https://github.com/gogo/protobuf
#Remember to install gogoprotoslick
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogoslick_out=. gsync.proto