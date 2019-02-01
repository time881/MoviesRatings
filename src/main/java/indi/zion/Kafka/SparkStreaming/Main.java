package indi.zion.Kafka.SparkStreaming;

public class Main {
    public static void main(String[] args){
        MoviesHotStatistic m = new MoviesHotStatistic();
        try {
            m.ExcuteWithDirect();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
