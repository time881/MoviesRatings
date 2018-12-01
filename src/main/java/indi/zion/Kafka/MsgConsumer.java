package indi.zion.Kafka;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            InputStream inStream = MsgConsumer.class.getResourceAsStream("Consumer.properties");
            props.load(inStream);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());
            inStream.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new KafkaConsumer<String, Rate>(props);
    }
    
    public void NewThreadPool(int NumofThread) {
        consumer = InitProp();
        consumer.subscribe(Arrays.asList(props.getProperty("TOPIC")));
        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(NumofThread);
        while(true) {
            ConsumerRecords<String, Rate> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                cachedThreadPool.submit(() ->{ConsumerAction(consumerRecord);});
                //Pending implement close Consume control
            }
        }
    }
    
    public void ConsumerAction(ConsumerRecord<String, String> consumerRecord) {
        System.out.format("ThreadID:%d\t%d\t%d\t%s\t%s\n", 
            Thread.currentThread().getId(), 
            consumerRecord.offset(), 
            consumerRecord.partition(), 
            consumerRecord.key(),
            consumerRecord.value());
    }
}
