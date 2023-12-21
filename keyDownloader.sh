#!/bin/bash

## TOPICの一覧は下記のコマンドで取得できます↓ ##
#result=`/usr/bin/kafka/bin/kafka-topics.sh --list --bootstrap-server blue-0101-vm-kafka1.teltis.com:9093 --command-config /usr/bin/kafka/config/client-ssl-auth.properties`
##

# 引数が空白か判定
if [ -n "$1" ]; then
  ##引数が存在する場合は、引数で指定された装置のkeyを取得する
  result=`/usr/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server blue-0101-vm-kafka1.teltis.com:9093 --topic $1 --consumer.config /usr/bin/kafka/config/client-ssl-auth.properties --from-beginning --timeout-ms 10000`
  for i in ${result}
  do
  if [[ "${i}" == *DataPipeline* ]];
  then
    echo ${i}
  fi
  done
else
  echo 引数
#   ## 引数で装置指定がなかった場合、すべての装置を取得する
#   result=`/usr/bin/kafka/bin/kafka-topics.sh --list --bootstrap-server blue-0101-vm-kafka1.teltis.com:9093 --command-config /usr/bin/kafka/config/client-ssl-auth.properties`
#   for i in ${result}
#   do
#   key=`/usr/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server blue-0101-vm-kafka1.teltis.com:9093 --topic ${i} --consumer.config /usr/bin/kafka/config/client-ssl-auth.properties --from-beginning --timeout-ms 10000`
#     for j in ${key}
#     do
#     if [[ "${j}" == *DataPipeline* ]];
#     then
#       echo ${j}
#     fi
#     done
#   done
fi
