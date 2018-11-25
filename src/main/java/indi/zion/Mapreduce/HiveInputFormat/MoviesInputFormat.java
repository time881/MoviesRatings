package indi.zion.Mapreduce.HiveInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.google.common.base.Charsets;

import indi.zion.Util.DataCleanUtil;
import indi.zion.Util.MovieDataCleanUtil;

public class MoviesInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        MulCharLineRecordReader lineReader = new MulCharLineRecordReader(recordDelimiterBytes);
        DataCleanUtil util = new MovieDataCleanUtil();
        lineReader.setUtil(util);
        return lineReader;
    }
}
