package indi.zion.Mapred.HiveInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.base.Charsets;

import indi.zion.Util.CommonDataCleanUtil;
import indi.zion.Util.DataCleanUtil;
import indi.zion.Util.MovieDataCleanUtil;

public class CommonInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        MulCharLineRecordReader lineReader = new MulCharLineRecordReader(job, (FileSplit) genericSplit,
                recordDelimiterBytes);
        DataCleanUtil util = new CommonDataCleanUtil();
        lineReader.setUtil(util);
        return lineReader;
    }
}
