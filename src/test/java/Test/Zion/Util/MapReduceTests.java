package Test.Zion.Util;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import indi.zion.Mapreduces.Maps.TopNMapperClass.RatingPerMovieMapper;
import indi.zion.Mapreduces.Reduces.TopNReduceClass.RatingPerMovieReducer;

public class MapReduceTests {
    MapDriver<LongWritable, Text, Text, ArrayPrimitiveWritable> mapDriver;
    ReduceDriver<Text, ArrayPrimitiveWritable, Text, org.apache.hadoop.io.DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, ArrayPrimitiveWritable, Text, org.apache.hadoop.io.DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {

        RatingPerMovieMapper mapper = new RatingPerMovieMapper();
        RatingPerMovieReducer reducer = new RatingPerMovieReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void TopNStrReduce() throws IOException, InterruptedException {
        Text value = new Text("64272::195::2::998530022");
        Text value2 = new Text("64235::195::2::998530069");
        Text value3 = new Text("64245::195::3::998530102");
        Text value4 = new Text("65275::195::5::998530113");

        mapReduceDriver.withInput(new LongWritable(), value)
        .withInput(new LongWritable(), value2)
        .withInput(new LongWritable(), value3)
        .withInput(new LongWritable(), value4)
                .withOutput(new Text("195"), new DoubleWritable(3)).
                runTest();

    }

}
