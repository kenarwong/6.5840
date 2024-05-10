#!/bin/bash

TMPDIR=mr-tmp/
EMPTYDIR=$(find $TMPDIR -type d -empty)
if [ -e $TMPDIR ] && [ -z $EMPTYDIR ]; then
  rm mr-tmp/*
fi

rm wc.so
go build -buildmode=plugin ../mrapps/wc.go

max=10
for i in `seq 1 $max`
do
    go run mrworker.go wc.so &
done
