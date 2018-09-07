package examples.ex1;

/**
 * kafka发送和接收消息的一个例子
 * 在运行之前先确保192.168.1.24,192.168.1.25,192.168.1.26上的zk和kafka都已启动
 * 启动zk的命令是在zk的安装目录下执行命令：bin/zkServer.sh start
 * 启动kafka的命令是在kafka的安装目录下执行命令：nohup bin/kafka-server-start.sh config/server.properties &
 */
public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
        producerThread.start();
        KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
        consumerThread.start();

    }
}
