zookeeper:
	bin/zookeeper-server-start.sh config/zookeeper.properties
server:
	bin/kafka-server-start.sh config/server.properties
listpart:
	./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe --topic provatop
	./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe --topic test
	./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe --topic