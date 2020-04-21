/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list | egrep -v "consumer_offsets|test|narrow|wide" | xargs -I % /kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic %
