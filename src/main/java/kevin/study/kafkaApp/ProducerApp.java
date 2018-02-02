package kevin.study.kafkaApp;

import kafka.serializer.DefaultEncoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: 2017/12/29
 * @ProjectName: kafkaApp
 */
public class ProducerApp implements Runnable,IKafkaConfig{

    private String topic ;
    private String key;
    private String value;

    public ProducerApp(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        KafkaProducer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);    //有多个重载方法，可指定分区
        Future<RecordMetadata> send = producer.send(record);
        try {
            RecordMetadata metadata = send.get();   //该方法再没有返回结果时会一直阻塞
            int partition = metadata.partition();
            long offset = metadata.offset();
            System.out.println("Send Key : " + this.key + " Value : " + this.value + " Partition : " + partition + " Offset : " + offset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private KafkaProducer<String, String> createProducer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect" , ZK_HOST);
        properties.put("bootstrap.servers" , KAFKA_BROKERS);
        properties.put("serializer.class",DefaultEncoder.class.getName());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String , String>(properties);
    }


    public static void main(String[] args) {
        System.out.println("生产线程启动...");
        String topic = "HB-Test";
        Random random = new Random();
        for (int i = 0; i < 10 ; i++) {
            int index = random.nextInt(5);
            String key = "K";
            String value = "This is message ...";
            key += index;
            value += i;
            new ProducerApp(topic , key , value).run();
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            System.out.println("******************************************");
        }
    }

}
