package indi.zion.Kafka;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import indi.zion.InfoStream.Beans.BeanDecoder;
import indi.zion.InfoStream.Beans.Rate;

public class MsgConsumer {
    private KafkaConsumer<String, Rate> consumer;
    private Properties props = new Properties();

    private KafkaConsumer InitProp() {
        try {
            InputStream inStream = MsgConsumer.class.getClassLoader().getResourceAsStream("Kafka/Consumer.properties");
            if (inStream == null) {
                try {
                    FileInputStream FileProducerIS = new FileInputStream(
                            "target//config_properties//Kafka//ReadPath.properties");

                    props.load(FileProducerIS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                props.load(inStream);
            }
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
            inStream.close();
            return new KafkaConsumer<String, Rate>(props);
        } catch (

        Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    // Consumer
    public void Consumer() {
        try {
            consumer = InitProp();
            Set<String> names = AdminClient.create(props).listTopics().names().get();
            if (names.contains(props.getProperty("TOPIC"))) {
                consumer.subscribe(Arrays.asList(props.getProperty("TOPIC")));
                while (true) {
                    try {
                        ConsumerRecords<String, Rate> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                        for (ConsumerRecord consumerRecord : consumerRecords) {
                            // Pending implement close Consume control
                            ConsumerAction(consumerRecord);
                        }
                    } catch (Exception e) {
                        // TODO: handle exception
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public void ConsumerAction(ConsumerRecord<String, Rate> consumerRecord) {
        System.out.format("ThreadID:%d\t%d\t%d\t%s\t%s\n", Thread.currentThread().getId(), consumerRecord.offset(),
                consumerRecord.partition(), consumerRecord.key(), consumerRecord.value().getMovieID());
    }
}
