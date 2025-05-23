##### Database & Streaming Utilities

This file contains a set of commands for working with PostgreSQL, Debezium, Kafka, Elasticsearch, and MongoDB.  
Each command is documented with comments explaining its purpose.

---

##### 1. PostgreSQL

##### 1.1. Connect to the database using password authentication

psql -d postgres_db -p 5432 -U postgres_user -W                    # prompt for password (postgres_password)

##### 1.2. Check loaded extensions

psql -U postgres_user -d postgres_db -c "SHOW shared_preload_libraries;"

##### Displays the list of libraries loaded at server startup
2. Debezium (PostgreSQL → Kafka)
2.1. Add a table to the publication

ALTER PUBLICATION dbz_publication
  ADD TABLE public.university;
-- Makes the public.university table available for Debezium CDC
3. Kafka
3.1. Console consumer for Debezium topic

docker exec -it broker bash -c " kafka-console-consumer --bootstrap-server broker:29092 --topic postgres_server.public.university --from-beginning --timeout-ms 10000"
##### Reads all messages from the 'university' topic, waiting up to 10 seconds
3.2. Console consumer with custom formatting

docker exec -it broker bash -c "kafka-console-consumer --bootstrap-server broker:9092 --topic postgres_server.public.students --from-beginning --max-messages 2 --formatter 'value=io.debezium.tools.StringConverter'"
##### Reads the first 2 messages from the 'students' topic and converts bytes to strings
4. Kafka Connect & Elasticsearch
4.1. Install the Elasticsearch sink connector

docker exec -it kafka-connect bash -c "confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:16.4.0\"
##### Installs the Elasticsearch sink connector version 16.4.0
4.2. Create connector via REST API

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @elastic_sink.json

##### Registers a connector defined by the elastic_sink.json configuration file
4.3. Check connector status
curl -s http://localhost:8083/connectors/elastic-sink-lectures-only/status | jq .
##### Retrieves the current status of the 'elastic-sink-lectures-only' connector
4.4. Delete connector

curl -X DELETE http://localhost:8083/connectors/elastic-sink-lectures-only
##### Removes the specified connector from Kafka Connect
5. Elasticsearch
5.1. List indices and document counts

curl -u elastic:secret -X GET "http://elasticsearch:9200/_cat/indices?v&h=index,docs.count"
##### Shows the list of indices and the number of documents in each
6. MongoDB
6.1. Connect and display a collection

docker exec -it mongodb mongosh "mongodb://admin:secret@localhost:27017/university_db" --eval 'db.universities.find().pretty()'
##### Connects to the 'university_db' database and pretty-prints all documents in the 'universities' collection
