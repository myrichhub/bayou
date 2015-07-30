#! /bin/bash

cd master
go build
mv master ../mst
cd ..
cd client
go build
mv client ../cli
cd ..
cd server
go build
mv server ../srv
cd ..
