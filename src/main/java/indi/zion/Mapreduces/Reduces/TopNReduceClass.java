package indi.zion.Mapreduces.Reduces;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReduceClass {

    // Calculate mean rate per movie
    public static class RatingPerMovieReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, DoubleWritable> {
        /*
         * (4168,(3.5,1.0))
         */
        @Override
        protected void reduce(Text arg0, Iterable<ArrayPrimitiveWritable> arg1,
                Reducer<Text, ArrayPrimitiveWritable, Text, DoubleWritable>.Context context) {
            // TODO Auto-generated method stub
            double sum = 0.0;
            double count = 0.0;
            for (ArrayPrimitiveWritable apw : arg1) {
                Object o = apw.get();
                if (apw.getComponentType().getName() == "double") {
                    sum += Array.getDouble(o, 0);
                    count += Array.getDouble(o, 1);
                }
            }
            try {
                context.write(arg0, new DoubleWritable((count - 0.0) < 1E-5 ? 0.0 : (sum / count)));
            } catch (IOException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // assume reduce with one thread one object
    public static class TopNMovieReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        private int N = 10;
        private volatile TreeMap<Double, Text> resultResultSet = new TreeMap<Double, Text>();

        @Override
        protected void reduce(DoubleWritable arg0, Iterable<Text> arg1,
                Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            arg1.forEach((Text MoveId) -> {
                resultResultSet.put(arg0.get(), new Text(MoveId));// Text be single as default, new for the case
                if (resultResultSet.size() > N) {
                    resultResultSet.remove(resultResultSet.firstKey());
                }
            });
        }

        @Override
        protected void cleanup(Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context) {
            // TODO Auto-generated method stub
            resultResultSet.forEach((Double Rate, Text MovieId) -> {
                try {
                    context.write(MovieId, new DoubleWritable(Rate));
                } catch (IOException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
        }
    }

    public static class JoinMovieReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> line, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            ArrayList<String> rates = new ArrayList();
            ArrayList<String> movies = new ArrayList();
            line.forEach((Text record) -> {
                String str = record.toString();
                int t = str.indexOf("\t");
                if ("01".equals(str.substring(0, 2))) {
                    rates.add(str.substring(str.lastIndexOf("\t") + 1));
                } else {

                    movies.add(str);
                }
            });
            for (String movie : movies) {
                for (String rate : rates) {
                    context.write(new Text(rate), new Text(movie.substring(movie.indexOf("\t") + 1)));
                }
            }
        }

        @Override
        // 排序名次
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            super.cleanup(context);
        }
    }
}
