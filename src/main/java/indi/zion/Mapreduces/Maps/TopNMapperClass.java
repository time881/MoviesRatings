package indi.zion.Mapreduces.Maps;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapperClass {
 
    // rating map solution get local top N record
    public static class RatingPerMovieMapper extends Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {

        // Rating map (offset,9809::4168::3.5::1091838156) to (4168,(3.5,1.0))
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] lineSplit = line.split("::");
            ArrayPrimitiveWritable NumVal = new ArrayPrimitiveWritable(double.class);
            double[] numbers = { Double.valueOf(lineSplit[2]), 1.0 };
            NumVal.set(numbers);

            context.write(new Text(lineSplit[1]), NumVal);
        }
    }

    // as N << count to calculate TopN, less than 1E4
    public static class TopNRatingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        // top 10
        private int N = 10;
        private volatile TreeMap<Double, Text> mapResultSet = new TreeMap<Double, Text>();

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
                throws IOException, InterruptedException {

            String[] kv = value.toString().split("\t");

            mapResultSet.put(Double.valueOf(kv[1]), new Text(kv[0]));

            if (mapResultSet.size() > N) {
                mapResultSet.remove(mapResultSet.firstKey());
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            mapResultSet.forEach((Double Rate, Text MovieId) -> {
                try {
                    context.write(new DoubleWritable(Rate), MovieId);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
        }
    }

    public static class MoviesTiltleCleanMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            super.map(key, value, context);
        }
    }

    public static class JoinMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            int index = line.indexOf(":");
            if (index != -1) {
                context.write(new Text(line.substring(0, index)),
                        new Text("02\t" + line.substring(index + 2).replaceAll("::", "\t")));
            } else {
                index = line.indexOf("\t");
                if (index != -1) {
                    context.write(new Text(line.substring(0, index)), new Text("01\t" + line.substring(index + 1)));
                }
            }
        }
    }
}
