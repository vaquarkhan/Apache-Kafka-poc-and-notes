package com.imaginea.mapreducejobs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.jsoup.Jsoup;

public class CountURLMapper extends MapReduceBase implements Mapper<Text, ArcFileItem, Text, Text> {
    StopWords stopWords = new StopWords();

    public void map(Text key, ArcFileItem value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        try {
            if (!value.getMimeType().contains("text")) {
                return;  // Only parse text.
            }
            // Retrieves page content from the passed-in ArcFileItem.
            ByteArrayInputStream inputStream = new ByteArrayInputStream(value.getContent().getReadOnlyBytes(), 0, value.getContent().getCount());
            // Converts InputStream to a String.
            String content = new Scanner(inputStream).useDelimiter("\\A").next();
            // Parses HTML with a tolerant parser and extracts all text.
            String pageText = Jsoup.parse(content).text();
            // Removes all punctuation.
            pageText = pageText.replaceAll("[^a-zA-Z ]", " ");
            // pageText = pageText.replaceAll("\\s+[0-9]+\\s+ ", " ");
            // Normalizes whitespace to single spaces.
            pageText = pageText.replaceAll("\\s+", " ");
            // Splits by space and outputs to OutputCollector.
            for (String word : pageText.split(" ")) {
                word = word.trim().toLowerCase();
                if (word.length() == 0 || stopWords.stopWordsSet.contains(word)) {
                    continue;
                }
                output.collect(new Text(word), new Text(value.getUri()));
            }
            reporter.getCounter("CountURLMapper.document", "doc_count").increment(1);
            reporter.setStatus("Processing doc:" + reporter.getCounter("CountURLMapper.document", "doc_count").getValue());
        } catch (Exception e) {
            e.printStackTrace();
            reporter.getCounter("CountURLMapper.exception", e.getClass().getSimpleName()).increment(1);
        }
    }
}
