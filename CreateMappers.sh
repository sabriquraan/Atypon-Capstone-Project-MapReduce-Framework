#!/bin/bash
#docker build . -t mapper
pwd=$(pwd)

for i in $(eval echo {$1..$2})
do
  docker run --rm --expose 7777 --network="host" -v "$(pwd):/dirc" mapper ./runMapper.sh $i $3 &
done
