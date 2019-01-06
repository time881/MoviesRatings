package indi.zion.Hbase.HbaseDao;

import indi.zion.Kafka.SparkStreaming.MoviesHotStatistic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Properties;

public class MoviesHotDaoImpl implements MoviesHotDao {
    private static Properties properties = new Properties();

    private void InitProperties() {
        try {
            InputStream inStream = MoviesHotStatistic.class.getClassLoader().getResourceAsStream("Hbase/HbaseConnect.properties");
            if (inStream == null) {
                FileInputStream FileInStream = new FileInputStream(
                        "target//config_properties//Hbase//HbaseConnect.properties");
                properties.load(FileInStream);
                inStream.close();
            } else {
                properties.load(inStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void GetWindowGraph(String tableName, String StartTimeStamp, String EndTimeStamp, String MovieID) {
        int caching = 20;
        int batch = 100;
        InitProperties();
        Configuration conf = HBaseConfiguration.create();
        properties.forEach((Object k, Object v) -> {
            conf.set((String) k, (String) v);
        });
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            try {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Scan scan = new Scan();
                scan.setCaching(caching);
                scan.setBatch(batch);
                List<Filter> filterlist = null;
                Filter timeFilterStart = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(StartTimeStamp.getBytes()));
                Filter timeFilterStop = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(String.valueOf(Long.parseLong(EndTimeStamp) + 1000).getBytes()));
                filterlist.add(timeFilterStart);
                filterlist.add(timeFilterStop);
                scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filterlist));
                ResultScanner results = table.getScanner(scan);
                for (Result result : results) {

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
