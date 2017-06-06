#! /bin/bash

echo "******** ARRANCA ZOOKEEPER **********"
/opt/zookeeper/bin/zkServer.sh start

echo "********* ARRANCA HBASE **********"
/opt/hbase/bin/start-hbase.sh

echo "******** ARRANCA KAFKA **********"
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
echo "-"
read -rst 1; timeout=$?
echo "--"
read -rst 1; timeout=$?
echo "---"
read -rst 1; timeout=$?
echo "----"
read -rst 1; timeout=$?
echo "----->OK"

echo "************  ARRANCA FLINK Server **********"
/opt/flink-1.2.0/bin/start-local.sh


#echo "********* ARRANCA HDFS ***********"
#./start-all.sh

echo "******** CREAMOS TOPICS **********"
/opt/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic inditex --partitions 1 --replication-factor 1

echo "******** ARRANCA Redis, Flink Y PRODUCTOR **********"

gnome-terminal --geometry=40x3+9000+9000 -e ~/MasterBD/Proyecto//redis.sh 
gnome-terminal --geometry=40x30+1000+9000 -e ~/MasterBD/Proyecto//flink.sh 

read -rst 10; timeout=$?

java -jar /home/sam/synthetic-producer-master/synthetic-producer-1.4.1-SNAPSHOT-selfcontained.jar -z 127.0.0.1:2181 -c ~/MasterBD/Proyecto/inditex.yml -r 1000 -t 1

echo "*********** APAGA REDIS y MATA KAFKA****************"
echo "Mata Kafka"
jps | grep Kafka | cut -d " " -f "1" | xargs kill -KILL
#echo "Mata Terminales"
#jps | grep ConsoleConsumer | cut -d " " -f "1" | xargs kill -KILL

#echo "*********** Eliminamos Topics ***********"
#/opt/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --delete --topic inditex


#echo "********ELIMINAMOS LOGS ANTIGUOS **********"
#rm -r -f /tmp/kafka-logs
#rm -r -f /opt/zookeeper/tmp/version-2

#read -rst 0.3; timeout=$?

echo "**************************Cierra Redis correctamente**********************"
/opt/redis/src/redis-cli shutdown

#echo "**************************Cierra Zookeeper y HBase****************************"
#/opt/zookeeper/bin/zkServer.sh stop
#/opt/hbase/bin/stop-hbase.sh

echo "------------------------------- TODO FINALIZADO OK -------------------------"
