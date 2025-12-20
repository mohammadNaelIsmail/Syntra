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

C:\kibana-8.19.8

bin\elasticsearch.bat

bin\kibana.bat


curl -X PUT "http://localhost:9200/people_snapshot" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "person_id": { "type": "keyword" },
      "name": { "type": "text" },
      "skills_before_list": { "type": "keyword" },
      "skills_after_list": { "type": "keyword" },
      "experiences": {
        "type": "nested",
        "properties": {
          "company_name": { "type": "keyword" },
          "date_from": { "type": "date" },
          "date_to": { "type": "date" }
        }
      },
      "last_update": { "type": "date" }
    }
  }
}
'
