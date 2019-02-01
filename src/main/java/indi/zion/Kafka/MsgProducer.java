package indi.zion.Kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import indi.zion.Constant.SpaceUnit;
import indi.zion.InfoStream.Beans.BeanEncoder;
import indi.zion.InfoStream.Beans.Rate;

public class MsgProducer {
    private Properties props = new Properties();
    private String RatePath;
    private String TagPath;
    private KafkaProducer<String, Rate> kfkprod;
    private ReadController<Rate> readController;
    private int readCycleTime = 10000 * 1000;
    private String requestSize = 0.1 + SpaceUnit.K;

    public MsgProducer() {
        InitProp();
        kfkprod = new KafkaProducer<String, Rate>(props);
        RatePath = props.getProperty("ratingPath");// one case
        readController = new ReadController<Rate>(RatePath, requestSize, Rate.class, 0);// pipe of reading beans, read
                                                                                      // requestedSize once
    }

    @SuppressWarnings("resource")
    public void InitProp() {
        try {
            InputStream TextIS = MsgProducer.class.getClassLoader().getResourceAsStream("Kafka/ReadPath.properties");
            InputStream ProducerIS = MsgProducer.class.getClassLoader().getResourceAsStream("Kafka/Producer.properties");
            if (TextIS == null || ProducerIS == null) {
                try {
                    // Read outside properties for package
                    FileInputStream FileTextIS = new FileInputStream("config_properties//Kafka//ReadPath.properties");
                    FileInputStream FileProducerIS = new FileInputStream(
                            "config_properties//Kafka//ReadPath.properties");
                    props.load(FileTextIS);
                    props.load(FileProducerIS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                props.load(TextIS);
                props.load(ProducerIS);
            }

            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", new BeanEncoder<Rate>().getClass().getName());
            TextIS.close();
            ProducerIS.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void SendMsg() {
        try {
            ArrayList<Rate> tmp;
            long leftTime;// count time of run one times
            CheckKafkaTopic(props.getProperty("bootstrap.servers"), props.getProperty("TOPIC"));
            while (true) {
                long currentTime = System.currentTimeMillis();
                tmp = readController.Read();
                if (tmp.size() != 0) {
                    for (Rate r : tmp) {
                        ProducerRecord<String, Rate> record = new ProducerRecord<String, Rate>(
                                props.getProperty("TOPIC"), r);
                        kfkprod.send(record, new SendCallback(record, 0));
                    }
                }
                leftTime = readCycleTime - (System.currentTimeMillis() - currentTime);
                System.out.println(leftTime);
                if (leftTime > 0) {
                    try {
                        Thread.sleep(leftTime);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }

    private void CheckKafkaTopic(String bootstrapServer, String topic) throws InterruptedException, ExecutionException {
        // TODO Auto-generated method stub
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        AdminClient adminClient = AdminClient.create(properties);
        ListTopicsResult listTopics = adminClient.listTopics();
        Set<String> names = listTopics.names().get();
        if (!names.contains(topic)) {
            List<NewTopic> topicList = new ArrayList<NewTopic>();
            Map<String, String> configs = new HashMap<String, String>();
            int partitions = 5;
            Short replication = 1;
            NewTopic newTopic = new NewTopic(topic, partitions, replication).configs(configs);
            topicList.add(newTopic);
            adminClient.createTopics(topicList);
        }
        adminClient.close();
    }

    /**
     * producer回调
     */
    static class SendCallback implements Callback {
        ProducerRecord<String, Rate> record;
        int sendSeq = 0;

        public SendCallback(ProducerRecord record, int sendSeq) {
            this.record = record;
            this.sendSeq = sendSeq;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // send success
            if (null == e) {
                String meta = "topic:" + recordMetadata.topic() + ", partition:" + recordMetadata.topic() + ", offset:"
                        + recordMetadata.offset();
                System.out.println("send message success, record:" + record.value().getMovieID() + ", meta:" + meta);
                return;
            }
            // send failed
            System.out.println("send message failed, seq:" + sendSeq + ", record:" + record.toString() + ", errmsg:"
                    + e.getMessage());
        }
    }
}
