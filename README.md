bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --partitions 1 --replication-factor 1
ahmad coments 
## 🚀 Running Apache Kafka (Windows) ahmad

### 1️⃣ Go to Kafka binaries directory
```bat
cd C:\kafka\kafka_2.13-3.7.2\bin\windows

.\zookeeper-server-start.bat ..\..\config\zookeeper.properties

.\kafka-server-start.bat ..\..\config\server.properties

.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic test --partitions 1 --replication-factor 1

.\kafka-console-consumer.bat --topic test --from-beginning --bootstrap-server localhost:9092

.\kafka-console-producer.bat --topic test --bootstrap-server localhost:9092

