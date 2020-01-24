#!/bin/bash
#docker build . -t reducer
pwd=$(pwd)
docker network rm ReducerNetwork  > /dev/null
docker network create --subnet=172.160.0.0/16 --gateway 172.160.0.1 ReducerNetwork  > /dev/null
for i in $(eval echo {$1..$2})
do
x=$(expr $i + 10)
  docker run --rm  --net ReducerNetwork --ip 172.160.0.$x  -v "$(pwd):/dirc" reducer ./runReducer.sh $i $2 &
done

