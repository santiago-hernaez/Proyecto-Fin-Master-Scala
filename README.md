# Proyecto-Fin-Master-Scala
Proyecto de Fin de Master realizado en Scala: Arquitectura Big Data para gestionar facturas de venta. 
Kafka-Flink-Redis-Cassandra

Utiliza el generador de JSON : Synthetic producer que se puede localizar y descargar en 
https://github.com/redBorder/synthetic-producer

El fichero de configuracion utilizado se puede localizar en Proyecto-Fin-Master-Scala/src/main/resources/inditex.yml

IMPRESCINDIBLE tener instalado e iniciado tanto el server de Kafka como el Redis.
el .dmp de Redis con los datos se ha incluido en Proyecto-Fin-Master-Scala/src/main/resources/dump.rdb

Clonar o descargar el repositorio  y compilar el jar con : 

mvn clean package -Pbuild-jar

Se puede ejecutar posteriormente directamente desde Eclipse o IntelliJ o con:

flink ./target/jarName.jar
iniciando previamente el cluster de Flink con ../flink-1.2/bin/start-local.sh
