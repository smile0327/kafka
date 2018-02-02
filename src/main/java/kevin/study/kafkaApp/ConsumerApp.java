package kevin.study.kafkaApp;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: 2017/12/29
 * @ProjectName: kafkaApp
 */
public class ConsumerApp implements Runnable , IKafkaConfig {

    private String topic;

    public ConsumerApp(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        ConsumerConnector connector = createConnector();
        //设计topic和stream的关系，即K为topic，V为stream的个数N
        //获取numThreads个stream
        Map<String , Integer> topicCountMap = new HashMap<>();
        //可设置多个topic，表示客户端可以同时消费多个topic
        //当有多个partition时，可将线程数设置为与partition数量一致，一个线程消费一个partition
        topicCountMap.put(topic , new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);
        for (KafkaStream<byte[] , byte[]> stream : kafkaStreams){
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while (iterator.hasNext()){
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                String topic = next.topic();
                String key = null;
                String message = null;
                try {
                    key = new String(next.key());
                    message = new String(next.message());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long offset = next.offset();
                int partition = next.partition();
                System.out.println("Consumer Topic : " + topic + " Key : " + key + " Value : " + message + " Offset : " + offset + " Partition : " + partition);
            }
        }

    }

    private ConsumerConnector createConnector(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect" , ZK_HOST);
        properties.put("metadata.broker.list" , KAFKA_BROKERS);
        //group 代表一个消费组
        properties.put("group.id", "test-group");
        //zk连接配置
        properties.put("zookeeper.session.timeout.ms", "4000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("auto.offset.reset", "smallest");
        //序列化类
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        System.out.println("启动消费线程...");
        String topic = "HB-Test";
        new ConsumerApp(topic).run();
    }

}
