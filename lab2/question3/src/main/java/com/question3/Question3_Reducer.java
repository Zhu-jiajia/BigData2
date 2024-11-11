package com.question3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Question3_Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> activeDates = new HashSet<>();
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String reportDate = parts[0];
            String isActive = parts[1];
            if ("1".equals(isActive)) {
                activeDates.add(reportDate);
            }
        }
        int activeCount = activeDates.size();
        result.set(String.valueOf(activeCount));
        context.write(key, result);
    }
}
