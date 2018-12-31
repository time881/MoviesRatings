package indi.zion.Kafka.SparkStreaming;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
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
    private Properties props = new Properties();
    private Properties hbaseProps = new Properties();

    private void InitProp() {
        try {
            InputStream inStream = MoviesHotStatistic.class.getResourceAsStream("Consumer.properties");
            if (inStream == null) {
                try {
                    FileInputStream FileInStream = new FileInputStream(
                            "config_properties//Kafka//SparkStream//Consumer.properties");
                    props.load(FileInStream);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                props.load(inStream);
            }
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
            inStream.close();
            
            InputStream hbaseInStream = MoviesHotStatistic.class.getResourceAsStream("HbaseConnect.properties");
            if (hbaseInStream == null) {
                try {
                    FileInputStream HbaseFileInStream = new FileInputStream(
                            "config_properties//Kafka//SparkStream//HbaseConnect.properties");
                    hbaseProps.load(HbaseFileInStream);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                hbaseProps.load(hbaseInStream);
            }
            inStream.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void ExcuteWithDirect() throws InterruptedException, ExecutionException {
        InitProp();
        Set<String> names = AdminClient.create(props).listTopics().names().get();
        if (names.contains(props.getProperty("TOPIC"))) {
            SparkConf conf = new SparkConf().setMaster((String) props.get("Spark.Master"))
                    .setAppName(this.getClass().getSimpleName().replaceAll("$", ""));
            JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
            Collection<String> topic = Arrays.asList(props.getProperty("TOPIC"));
            Configuration config = new Configuration();
            hbaseProps.forEach((t, u) -> {
                config.set(String.valueOf(t), String.valueOf(u));
            });;
            Map<String, Object> kafkaParams = new HashMap<>();

            kafkaParams.put("group.id", props.get("group.id"));
            kafkaParams.put("auto.offset.reset", props.get("auto.offset.reset"));
            kafkaParams.put("bootstrap.servers", props.get("bootstrap.servers"));
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", new BeanDecoder<Rate>().getClass());
            kafkaParams.put("enable.auto.commit", false);

            JavaInputDStream<ConsumerRecord<String, Rate>> records = KafkaUtils.createDirectStream(jsc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, Rate>Subscribe(topic, kafkaParams));

            JavaPairDStream<Integer, Tuple2<Double, Integer>> kv = StatisticTool.MapToRateWithCount(records);
            JavaPairDStream<Integer, Tuple2<Double, Integer>> result = StatisticTool.MapPairToCount(kv);
            JavaPairDStream<Integer, Double> MeanByMID = StatisticTool.MeanRate(result);
            StatisticTool.PrintOut(MeanByMID, config);
            jsc.start();
            jsc.awaitTermination();
            jsc.stop();
        }
    }
}

//can use with protobuf
class StatisticTool implements Serializable {

    private static final long serialVersionUID = 5847840884326070434L;

    public static JavaPairDStream<Integer, Tuple2<Double, Integer>> MapToRateWithCount(
            JavaInputDStream<ConsumerRecord<String, Rate>> records) {
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
        return kv;
    }

    public static JavaPairDStream<Integer, Tuple2<Double, Integer>> MapPairToCount(
            JavaPairDStream<Integer, Tuple2<Double, Integer>> records) {
        JavaPairDStream<Integer, Tuple2<Double, Integer>> result = records.reduceByKey(
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    private static final long serialVersionUID = 4366888589402797333L;

                    @Override
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2)
                            throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<Double, Integer>(v1._1 + v2._1, v1._2 + v2._2);
                    }
                });
        return result;
    }

    public static JavaPairDStream<Integer, Double> MeanRate(JavaPairDStream<Integer, Tuple2<Double, Integer>> records) {
        JavaPairDStream<Integer, Double> MeanByMID = records
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double>() {
                    private static final long serialVersionUID = 5808418490663946948L;

                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> t) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<Integer, Double>(t._1, t._2._1 / t._2._1);
                    }
                });
        return MeanByMID;
    }

    public static void PrintOut(JavaPairDStream<Integer, Double> results, Configuration config) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(config);
        JavaHBaseContext hbaseContext;
        
        results.foreachRDD(new VoidFunction<JavaPairRDD<Integer, Double>>() {
            private static final long serialVersionUID = -2264563798376792125L;

            @Override
            public void call(JavaPairRDD<Integer, Double> rdd) throws Exception {
                // TODO Auto-generated method stub
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Double>>>() {

                    private static final long serialVersionUID = 240640375195965315L;

                    @Override
                    public void call(Iterator<Tuple2<Integer, Double>> t) throws Exception {
                        // TODO Auto-generated method stub

                    }
                });
            }
        });
    }
}
