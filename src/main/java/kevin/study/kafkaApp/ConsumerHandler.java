package kevin.study.kafkaApp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 13:52 2018/3/29
 * @ProjectName: kafkaApp
 */
public class ConsumerHandler implements Runnable,IKafkaConfig {

    private String topic;

    public ConsumerHandler(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = createConsumer(topic);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.isEmpty()){
                System.out.println("读取消息为空...");
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                System.out.println("Key : " + key + " Value : " + value);
            }
        }
    }

    private KafkaConsumer<String,String> createConsumer(String topic){
        KafkaConsumer<String,String> consumer;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) {
        System.out.println("启动消费端...");
        String topic = "SQL-Topic";
        new ConsumerHandler(topic).run();
    }

}
