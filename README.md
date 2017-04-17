## Master project - Big Data Architecture to manage sales tickets.

Scala project that receives purchase tickets and enriches them with data from Redis. The enriched data is processed to obtain KPIs and metrics and stored on Hbase.
This project is based on Inditex world structure comprising 8 different branches (Zara, Oysho, Pull&Bear, Stradivarius, Bershka, Lefties, Uterqüe and Massimo Dutti) and a total of 7984 stores around the world.
The stores database is based on real data from the Inditex stores.
The clothes database was created from scratch with tailored data.

### Prerequisites:

* Flink compatible OS
* Java (version 8)
* Scala (version 2.12.1)
* Apache Flink (version 1.2)
* Apache Kafka (version 0.10.1.1)
* Redis (version 3.2.8)
* Apache Hbase (version 1.2)
* Maven (version 3.3.9) (only for building)

Kafka server, Redis server and Hbase server must be installed and running previous to starting flink.

Kafka installation and configuration available on https://kafka.apache.org/quickstart

Redis installation and configuration guides are available on https://redis.io/download

Redis .dmp file with Store list and Clothing database is available on Proyecto-Fin-Master-Scala/src/main/Files/dump.rdb

Hbase installation and configuration guides are available on https://hbase.apache.org/index.html

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

articulosComprados : array of purchased items with format [id : price, id : price, ...]. The id is used to access Redis and enrich data with clothing color, size, name, model, type and benefit.

### Build library

Clone or download repository and compile with:
```shell
git clone https://github.com/santiago-hernaez/Proyecto-Fin-Master-Scala.git
cd Proyecto-Fin-Master-Scala
mvn clean package -Pbuild-jar
<Flink-installation-folder>/bin/start-local.sh
<Flink-instalation-folder>/bin/flink run <Job-package-path>/target/Flink-1.0-SNAPSHOT.jar
```
Example:
```shell
/opt/flink-1.2/bin/flink run target/Flink-1.0-SNAPSHOT.jar
```
### Documentation

This project has been developed using v1.2.0 https://ci.apache.org/projects/flink/flink-docs-release-1.2/

The documentation for Apache Flink is located on the website: http://flink.apache.org

The documentation for Apache Kafka is located on : https://kafka.apache.org/documentation/

The documentation for Redis is located on : https://redis.io/documentation

The documentation for Hbase is located on : https://hbase.apache.org/book.html

