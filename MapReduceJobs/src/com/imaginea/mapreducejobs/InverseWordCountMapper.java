package com.imaginea.mapreducejobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * No of words for a count.
 */
public class InverseWordCountMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private final LongWritable ONE = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] counts = value.toString().split("\t");

            if (counts.length == 2 && counts[1].trim().length() > 0) {
                Long wordCount = Long.parseLong(counts[1]);
                context.write(new LongWritable(wordCount), ONE);
            } else {
                context.getCounter("InverseWordCountMapper.anomalies", "invalidKV").increment(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            context.getCounter("InverseWordCountMapper.exception", e.getClass().getCanonicalName()).increment(1);
        }
    }
}