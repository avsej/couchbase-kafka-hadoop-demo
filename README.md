# Couchbase-Kafka Integration Demo

## Environment

Download [HDP 2.2](http://hortonworks.com/hdp/downloads/). Before
starting, make sure that Zookeeper (`2181`) and Kafka (`6667`) ports
forwarded.

Start the virtual machine. Navigate to
[Ambari console](http://localhost:8080/) and make sure kafka service
started.

## Build

Compile jar file:

    ./gradlew shadowJar

Execute demo application, which listens all changes in default bucket
and posts corresponding messages in kafka.

    java -cp build/libs/couchbase-kafka-hadoop-demo-1.0-all.jar example.ClickStreamExample
