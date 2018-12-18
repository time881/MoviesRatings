package indi.zion.Kafka.SparkStreaming;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import indi.zion.InfoStream.Beans.BeanDecoder;
import indi.zion.InfoStream.Beans.Rate;
import indi.zion.Kafka.MsgConsumer;
import scala.Tuple2;

public class MoviesHotStatistic {
    private Properties props = new Properties();

    private void InitProp() {
        try {
            InputStream inStream = MsgConsumer.class.getResourceAsStream("Consumer.properties");
            props.load(inStream);
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
            inStream.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void ExcuteWithDirect(String Topic) throws InterruptedException, ExecutionException {
        Set<String> names = AdminClient.create(props).listTopics().names().get();
        if (names.contains(Topic)) {
            SparkConf conf = new SparkConf().setMaster("hserver1").setAppName("Demo");
            JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
            Collection<String> topic = Arrays.asList(Topic);
            Map<String, Object> kafkaParams = new HashMap<>();

            kafkaParams.put("metadata.broker.list", props.get("metadata.broker.list"));
            kafkaParams.put("group.id", props.get("metadata.broker.list") + "_forHotStatistic");
            kafkaParams.put("auto.offset.reset", props.get("auto.offset.reset"));
            kafkaParams.put("enable.auto.commit", false);
            kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
            kafkaParams.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());

            JavaInputDStream<ConsumerRecord<String, Rate>> records = KafkaUtils.createDirectStream(jsc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, Rate>Subscribe(topic, kafkaParams));

            JavaPairDStream<Integer, Tuple2<Double, Integer>> kv = records
                    .mapToPair(new PairFunction<ConsumerRecord<String, Rate>, Integer, Tuple2<Double, Integer>>() {
                        private static final long serialVersionUID = -1805595501933568605L;

                        @Override
                        public Tuple2<Integer, Tuple2<Double, Integer>> call(ConsumerRecord<String, Rate> record)
                                throws Exception {
                            // TODO Auto-generated method stub
                            return new Tuple2<Integer, Tuple2<Double, Integer>>(record.value().getMovieID(),
                                    new Tuple2<Double, Integer>(record.value().getRate(), 1));
                        }
                    }).cache();
            JavaDStream<Long> count = kv.count();
            JavaPairDStream<Integer, Tuple2<Double, Integer>> result = kv.reduceByKey(
                    new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                        private static final long serialVersionUID = 4366888589402797333L;

                        @Override
                        public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2)
                                throws Exception {
                            // TODO Auto-generated method stub
                            return new Tuple2<Double, Integer>(v1._1 + v2._1, v1._2 + v2._2);
                        }
                    });
            JavaPairDStream<Integer, Double> MeanByMID = result
                    .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double>() {
                        private static final long serialVersionUID = 5808418490663946948L;

                        @Override
                        public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> t)
                                throws Exception {
                            // TODO Auto-generated method stub
                            return new Tuple2<Integer, Double>(t._1, t._2._1 / t._2._1);
                        }
                    });

            MeanByMID.foreachRDD(new VoidFunction<JavaPairRDD<Integer,Double>>() {
                private static final long serialVersionUID = -2264563798376792125L;

                @Override
                public void call(JavaPairRDD<Integer, Double> rdd) throws Exception {
                    // TODO Auto-generated method stub
                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer,Double>>>() {
                        
                        @Override
                        public void call(Iterator<Tuple2<Integer, Double>> t) throws Exception {
                            // TODO Auto-generated method stub
                            
                        }
                    });
                }
            });
            jsc.start();
            jsc.awaitTermination();
            jsc.stop();
        }
    }
}
