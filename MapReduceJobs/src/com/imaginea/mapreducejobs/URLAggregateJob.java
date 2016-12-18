package com.imaginea.mapreducejobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;

public class URLAggregateJob {
    public static class URLAggregaterReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int count = 0;
            Set<String> set = new HashSet<String>();
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                if (count > 100)
                    break;
                String urls = values.next().toString().trim();
                // As this will also act as combiner.
                for (String url : urls.split(",")) {
                    if (url.length() > 0) {
                        set.add(url);
                        count++;
                    }
                }
            }
            for (String word : set) {
                sb.append(word).append(",");
            }
            output.collect(key, new Text(sb.toString()));
        }
    }

    /**
     * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
     */
    private static final String CC_BUCKET = "aws-publicdatasets";

    public static void main(String[] args) throws IOException {
        // Parses command-line arguments.
        String awsCredentials = args[0];
        String awsSecret = args[1];
        String inputPrefixes = "common-crawl/" + args[2];
        String outputFile = args[3];

        // Echoes back command-line arguments.
        System.out.println("Using AWS Credentials: " + awsCredentials);
        System.out.println("Using S3 bucket paths: " + inputPrefixes);

        // Creates a new job configuration for this Hadoop job.
        JobConf conf = new JobConf();

        // Configures this job with your Amazon AWS credentials
        conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
        conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
        conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
        conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);

        // Configures where the input comes from when running our Hadoop job,
        // in this case, gzipped ARC files from the specified Amazon S3 bucket
        // paths.
        ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
        ARCInputFormat inputFormat = new ARCInputFormat();
        inputFormat.configure(conf);
        conf.setInputFormat(ARCInputFormat.class);

        // Configures what kind of Hadoop output we want.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Configures where the output goes to when running our Hadoop job.
        // CSVOutputFormat.setOutputPath(conf, new Path(outputFile));
        FileOutputFormat.setOutputPath(conf, new Path(outputFile));
        // CSVOutputFormat.setCompressOutput(conf, false);
        conf.setOutputFormat(TextOutputFormat.class);

        // Allows some (10%) of tasks fail; we might encounter the
        // occasional troublesome set of records and skipping a few
        // of 1000s won't hurt counts too much.
        conf.set("mapred.max.map.failures.percent", "10");
        conf.set("mapred.textoutputformat.separator", "\t");
        // Tells the user some context about this job.
        InputSplit[] splits = inputFormat.getSplits(conf, 0);
        if (splits.length == 0) {
            System.out.println("ERROR: No .ARC files found!");
            return;
        }
        System.out.println("Found " + splits.length + " InputSplits:");
        for (InputSplit split : splits) {
            System.out.println(" - will process file: " + split.toString());
        }

        // Tells Hadoop what Mapper and Reducer classes to use;
        // uses combiner since reduce is associative and commutative.
        conf.setMapperClass(CountURLMapper.class);
        conf.setCombinerClass(URLAggregaterReducer.class);
        conf.setReducerClass(URLAggregaterReducer.class);

        // Tells Hadoop mappers and reducers to pull dependent libraries from
        // those bundled into this JAR.
        conf.setJarByClass(URLAggregateJob.class);

        // Runs the job.
        JobClient.runJob(conf);
    }
}
