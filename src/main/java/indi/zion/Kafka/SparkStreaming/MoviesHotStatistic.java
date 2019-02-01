package indi.zion.Kafka.SparkStreaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.junit.Test;
import scala.Tuple2;

public class MoviesHotStatistic {
    private Properties props = new Properties();
    private Properties hbaseProps = new Properties();

    private void InitProp() {
        try {
            InputStream inStream = MoviesHotStatistic.class.getClassLoader().getResourceAsStream("Spark/Consumer.properties");
            if (inStream == null) {
                try {
                    FileInputStream FileInStream = new FileInputStream(
                            "target//config_properties//Kafka//SparkStreaming//Consumer.properties");
                    if (FileInStream != null) {
                        props.load(FileInStream);
                        FileInStream.close();
                    } else {
                        throw new Exception("no property file being found");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                props.load(inStream);
                inStream.close();
            }
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", new BeanDecoder<Rate>().getClass().getName());
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
            InputStream hbaseInStream = MoviesHotStatistic.class.getClassLoader().getResourceAsStream("Hbase/HbaseConnect.properties");
            if (hbaseInStream == null) {
                try {
                    FileInputStream HbaseFileInStream = new FileInputStream(
                            "target//config_properties//Hbase//HbaseConnect.properties");
                    if (HbaseFileInStream != null) {
                        hbaseProps.load(HbaseFileInStream);
                        HbaseFileInStream.close();
                    } else {
                        throw new Exception("no property file being found");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                hbaseProps.load(hbaseInStream);
                hbaseInStream.close();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * hbase table name: MoviesHotMeanRate
     * table structure: "family", "movieID", "movieRate", "time"
     */
    public void ExcuteWithDirect() throws InterruptedException, ExecutionException {
        //Define the table structure
        String[] HbaseTable = new String[]{"MoviesHotMeanRate", "family", "movieID", "movieRate", "time"};
        String[] HbaseTableTest = new String[]{"MoviesHotMeanRateTest", "family", "movieID", "movieRate", "time"};
        InitProp();
        Set<String> names = AdminClient.create(props).listTopics().names().get();
        if (names.contains(props.getProperty("TOPIC"))) {
            SparkConf conf = new SparkConf()//.setMaster("local")//.setMaster((String) props.get("Spark.Master"))
                    .setAppName(this.getClass().getSimpleName().replaceAll("$", ""));
                   // .setJars(new String[]{"D:\\WorkBenchForIDE\\MoviesRatings\\classes\\artifacts\\MoviesRatings_jar\\MoviesRatings.jar"});
            JavaSparkContext js = new JavaSparkContext(conf);
            JavaStreamingContext jsc = new JavaStreamingContext(js, Durations.seconds(3));
            Collection<String> topic = Arrays.asList(props.getProperty("TOPIC"));
            Configuration config = new Configuration();
            hbaseProps.forEach((t, u) -> {
                config.set(String.valueOf(t), String.valueOf(u));
            });

            Map<String, Object> kafkaParams = new HashMap<>();

            kafkaParams.put("group.id", props.get("group.id"));
            kafkaParams.put("auto.offset.reset", props.get("auto.offset.reset"));
            kafkaParams.put("metadata.broker.list", props.get("bootstrap.servers"));
            kafkaParams.put("bootstrap.servers", props.get("bootstrap.servers"));
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", new BeanDecoder<Rate>().getClass());
            kafkaParams.put("enable.auto.commit", props.get("enable.auto.commit"));
            Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition((String)props.getProperty("TOPIC"), 0), 2L);
            JavaInputDStream<ConsumerRecord<String, Rate>> records = KafkaUtils.createDirectStream(
                    jsc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, Rate>Subscribe(topic, kafkaParams));
            JavaPairDStream<Integer, Tuple2<Double, Integer>> kv = StatisticTool.MapToRateWithCount(records);
            JavaPairDStream<Integer, Tuple2<Double, Integer>> result = StatisticTool.MapPairToCount(kv);
            JavaDStream<String> MeanByMID = StatisticTool.MeanRate(result);
            try {
                StatisticTool.PrintOut(MeanByMID, config, HbaseTableTest);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        JavaPairDStream<Integer, Tuple2<Double, Integer>> kv1 = records
                .mapToPair(new PairFunction<ConsumerRecord<String, Rate>, Integer, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<Double, Integer>> call(ConsumerRecord<String, Rate> record) throws Exception {
                        return new Tuple2<Integer, Tuple2<Double, Integer>>(record.value().getMovieID(),
                                new Tuple2<Double, Integer>(record.value().getRate(), 1));
                    }
                });
        return kv1;
    }

    public static JavaPairDStream<Integer, Tuple2<Double, Integer>> MapPairToCount(
            JavaPairDStream<Integer, Tuple2<Double, Integer>> records) {
        JavaPairDStream<Integer, Tuple2<Double, Integer>> result = records.reduceByKey(
                (Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) ->
                {
                    return new Tuple2<Double, Integer>(v1._1 + v2._1, v1._2 + v2._2);
                });
        return result;
    }

    public static JavaDStream<String> MeanRate(JavaPairDStream<Integer, Tuple2<Double, Integer>> records) {
        String TimeStampKey = StringUtils.leftPad(String.valueOf(System.currentTimeMillis()), 15, '0');
        JavaDStream<String> MeanByMID = records.map((Tuple2<Integer, Tuple2<Double, Integer>> t) -> {
            String RowKey = TimeStampKey + "-01-" + t._1;
            return RowKey + ":" + t._1 + ":" + t._2._1 / t._2._1;
        });
        return MeanByMID;
    }


    public static void PrintOut(JavaDStream<String> results, Configuration config, String[] HbaseTableInfo) throws IOException {
        results.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        Configuration configuration = HBaseConfiguration.create();
                        Connection connection = ConnectionFactory.createConnection(configuration);
                        Table table = connection.getTable(TableName.valueOf(HbaseTableInfo[0]));
                        List<Put> puts = new ArrayList<Put>();
                        stringIterator.forEachRemaining(
                                new Consumer<String>() {
                                    @Override
                                    public void accept(String record) {
                                        String[] keyValue = record.split(":");
                                        Put put = new Put(keyValue[0].getBytes());
                                        put.addColumn(HbaseTableInfo[1].getBytes(), HbaseTableInfo[2].getBytes(), keyValue[1].getBytes());
                                        put.addColumn(HbaseTableInfo[1].getBytes(), HbaseTableInfo[3].getBytes(), keyValue[2].getBytes());
                                        put.addColumn(HbaseTableInfo[1].getBytes(), HbaseTableInfo[4].getBytes(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()).getBytes());
                                        puts.add(put);
                                    }
                                }
                        );
                        table.put(puts);
                    }
                });

                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String record) throws Exception {

                    }
                });
            }
        });
                /*
        results.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                });
            }
        });*/
    }
}
