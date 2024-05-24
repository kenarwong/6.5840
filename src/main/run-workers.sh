#!/bin/bash

WORKER_COUNT=10
if [ ! $# -lt 1 ]; then
    WORKER_COUNT=$1
fi

if [ -z "$MR_MAPPER" ]; then
  export MR_MAPPER=wc
fi

TMPDIR=mr-tmp
[ -d "mr-tmp" ] || mkdir $TMPDIR
#if [ ! -d "mr-tmp" ]; then
#  mkdir $TMPDIR
#fi

EMPTYDIR=$(find $TMPDIR -type d -empty)
if [ -e $TMPDIR ] && [ -z $EMPTYDIR ]; then
  rm $TMPDIR/*
fi


if [ -f $MR_MAPPER.so ]; then
  rm $MR_MAPPER.so
fi
go build -buildmode=plugin ../mrapps/$MR_MAPPER.go

for i in `seq 1 $WORKER_COUNT`
do
    go run mrworker.go wc.so &
done
