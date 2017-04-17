## Master project - Big Data Architecture to manage sales tickets.

### Data origin

The data is originated by means of Json:Synthetic Producer : https://github.com/redBorder/synthetic-producer

Configuration file may be found in Proyecto-Fin-Master-Scala/src/main/Files/inditex.yml
The JSON entry format is as follows:
 ```scala
 {"id_tienda":1234,"fecha":123456789,"metodoPago":"tarjeta","articulosComprados":[12345:12.95,12346:128.45]}
 ```
id_tienda : code identifying shop location, chain, country and region (data available on Redis database)

fecha : Timestamp

metodoPago: Payment method used on the purchase (Credit card, cash, gift card, app or affinity card)

articulosComprados : array of purchased items with format [id : price, id:price,...] The id is used to access Redis and enrich data with clothing color, size, name, model, type and benefit.

### Prerequisites:

Kafka server and Redis server must be installed and running previous to starting flink.

Kafka installation and configuration available on https://kafka.apache.org/quickstart

Redis installation and configuration guides on https://redis.io/download and https://redis.io/documentation

Redis .dmp file with Shop list and Clothing database is available on Proyecto-Fin-Master-Scala/src/main/Files/dump.rdb

### Installation

Clone or download repository and compile with:

mvn clean package -Pbuild-jar

After successful compilation, start Flink cluster with:
../flink-1.2/bin/start-local.sh

And run compiled jar with:
flink run ./target/jarName.jar 

### Documentation

This project has been developed using v1.2.0 https://ci.apache.org/projects/flink/flink-docs-release-1.2/

The documentation for Apache Flink is located on the website: http://flink.apache.org

The documentation for Apache Kafka is located on : https://kafka.apache.org/documentation/

The documentation for Redis is located on : https://redis.io/documentation

The documentation for Hbase is located on : https://hbase.apache.org/book.html

