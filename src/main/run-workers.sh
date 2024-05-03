#!/bin/bash

rm mr-tmp/*
rm wc.so
go build -buildmode=plugin ../mrapps/wc.go

max=10
for i in `seq 0 $max`
do
    go run mrworker.go wc.so &
done