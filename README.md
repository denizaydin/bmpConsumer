# bmpConsumer
bmpConsumer 

- reads gobmp messages from kafka
- tries to create topology, nodes and edges for the data center
- caches the nodes and edges into a cache database as messages have repated information
- add new information into a graph database

- internal/kafka/consumer.go : must be edited for each use case. Currently it is written for 3 level clos for ip based interfabric
- pkg/message/type.go : must match json data format in the kafka, thus gobmp project.

