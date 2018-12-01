package indi.zion.Kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

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
    private ReadController readController;
    
    public MsgProducer() {
      InitProp();
      kfkprod = new KafkaProducer<String, Rate>(props);
      RatePath = props.getProperty("ratingPath");//one case
      readController = new ReadController<Rate>(RatePath, 3+SpaceUnit.M, Rate.class, 0);//pipe of reading beans, read requestedSize once 
    }
    
    public void InitProp() {
        try {
            InputStream TextIS = MsgProducer.class.getResourceAsStream("Read.properties");
            InputStream ProducerIS = MsgProducer.class.getResourceAsStream("Producer.properties");
            props.load(TextIS);
            props.load(ProducerIS);
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
        ArrayList<Rate> tmp;
        long leftTime;//count time of run one times
        while(true) {
            long currentTime=System.currentTimeMillis();
            tmp = readController.Read();
            if(tmp.size() != 0) {
                for(Rate r : tmp) {
                    ProducerRecord<String, Rate> record = new ProducerRecord<String, Rate>(props.getProperty("TOPIC"),r);
                    kfkprod.send(record, new SendCallback(record, 0));
                }
            }
            leftTime = 1 - (System.currentTimeMillis() - currentTime);
            if(leftTime > 0) {
                try {
                    Thread.sleep(leftTime);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * producer回调
     */
    static class SendCallback implements Callback {
        ProducerRecord<String, String> record;
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
                System.out.println("send message success, record:" + record.toString() + ", meta:" + meta);
                return;
            }
            // send failed
            System.out.println("send message failed, seq:" + sendSeq + ", record:" + record.toString() + ", errmsg:"
                    + e.getMessage());
        }
    }
}
