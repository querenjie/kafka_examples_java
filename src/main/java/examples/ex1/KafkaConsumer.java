package examples.ex1;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;
    public KafkaConsumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }
    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));   //第二个参数是partition数量
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
//        ConsumerIterator<byte[], byte[]> it = stream.iterator();
//        while (it.hasNext()) {
//            System.out.println("receive：" + new String(it.next().message()));
//            try {
//                sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        List<KafkaStream<byte[], byte[]>> partitions = consumerMap.get(topic);
        if (partitions != null) {
            for (KafkaStream<byte[], byte[]> onePartition : partitions) {
                ConsumerIterator<byte[], byte[]> it = onePartition.iterator();
                while (it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> message = it.next();
                    String topic = message.topic();
                    int partition = message.partition();
                    long offset = message.offset();
                    String key = message.key() == null ? "" : new String(message.key());
                    String msg = message.message() == null ? "" : new String(message.message());
                    // 在这里处理消息,这里仅简单的输出
                    // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
                    System.out.println( " thread : " + Thread.currentThread().getName()
                            + ", topic : " + topic + ", partition : " + partition + ", offset : " + offset + " , key : "
                            + key + " , message : " + msg);
                }
            }
        }
    }
}
