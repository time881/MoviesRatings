package Test.Zion.Util;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import indi.zion.Kafka.SparkStreaming.MoviesHotStatistic;


public class SparkStreamTest {
    @Test
    public void MoviesHotStatisitc() {
        MoviesHotStatistic mhs = new MoviesHotStatistic();
        try {
            mhs.ExcuteWithDirect();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
