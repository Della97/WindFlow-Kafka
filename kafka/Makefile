zookeeper:
	bin/zookeeper-server-start.sh config/zookeeper1.properties
server:
	bin/kafka-server-start.sh config/server1.properties
server1:
	bin/kafka-server-start.sh config/server2.properties
zookeeper1:
	bin/zookeeper-server-start.sh config/zookeeper1.properties
listpart:
	./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe --topic input
	./bin/kafka-topics.sh --bootstrap-server=localhost:9093 --describe --topic output
