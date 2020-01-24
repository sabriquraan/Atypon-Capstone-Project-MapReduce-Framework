#!/bin/bash
#docker build . -t reducer
pwd=$(pwd)
#docker network disconnect --force <network> release_diyaserver_1
docker network rm ReducerNetwork
docker network create --subnet=172.160.0.0/16 --gateway 172.160.0.1 ReducerNetwork
for i in $(eval echo {$1..$2})
do
x=$(expr $i + 10)
  docker run --rm  --net ReducerNetwork --ip 172.160.0.$x  -v "$(pwd):/dirc" reducer ./runReducer.sh $i $2 &
done

