package indi.zion.Kafka.SparkStreaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import indi.zion.InfoStream.Beans.BeanDecoder;
import indi.zion.InfoStream.Beans.Rate;
import scala.Tuple2;

public class MoviesHotStatistic {
    public void ExcuteWithDirect(String Topic) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("hserver1").setAppName("Demo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
        Collection<String> topic = Arrays.asList(Topic);
        Map<String, Object> kafkaParams = new HashMap<>();
        
        kafkaParams.put("metadata.broker.list", "hserver1:9092,hserver2:9092,hserver3:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());
        kafkaParams.put("group.id", "Rate_HotStatistic");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        
        JavaInputDStream<ConsumerRecord<String, Rate>> records = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, Rate>Subscribe(topic, kafkaParams));
        JavaPairDStream<Integer, Tuple2<Double, Integer>> kv = records.mapToPair(new PairFunction<ConsumerRecord<String,Rate>, Integer, Tuple2<Double, Integer>>() {
            private static final long serialVersionUID = -1805595501933568605L;
            @Override
            public Tuple2<Integer, Tuple2<Double, Integer>> call(ConsumerRecord<String, Rate> record) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<Integer, Tuple2<Double,Integer>>(record.value().getMovieID(), new Tuple2<Double,Integer>(record.value().getRate(), 1));
            }
        }).cache();
        JavaDStream<Long> count = kv.count();
        JavaPairDStream<Integer, Tuple2<Double, Integer>> result = kv.reduceByKey(new Function2<Tuple2<Double,Integer>, Tuple2<Double,Integer>, Tuple2<Double,Integer>>() {
            private static final long serialVersionUID = 4366888589402797333L;
            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<Double, Integer>(v1._1+v2._1, v1._2+v2._2);
            }
        });
        JavaPairDStream<Integer, Double> MeanByMID = result.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Double,Integer>>, Integer, Double>() {
            private static final long serialVersionUID = 5808418490663946948L;
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> t) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<Integer, Double>(t._1, t._2._1/t._2._1);
            }
        });
        
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
