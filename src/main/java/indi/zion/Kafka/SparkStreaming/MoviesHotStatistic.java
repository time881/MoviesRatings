package indi.zion.Kafka.SparkStreaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

public class MoviesHotStatistic {
    public void ExcuteWithDirect(String Topic) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("hserver1").setAppName("Demo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));

        Set<String> topic = new HashSet<String>();
        topic.add(Topic);
        Map<String, String> blocks = new HashMap<String, String>();
        blocks.put("metadata.broker.list", "hserver1:9092,hserver2:9092,hserver3:9092");

       // JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc, topic, blocks);

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
