package com.question1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Question1_Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sumInflow = 0.0;
        double sumOutflow = 0.0;
        for (Text value : values) {
            String[] funds = value.toString().split(",");
            try {
                sumInflow += Double.parseDouble(funds[0]);
                sumOutflow += Double.parseDouble(funds[1]);
            } catch (NumberFormatException e) {
                // Skip invalid input
            }
        }
        result.set(sumInflow + "," + sumOutflow);
        context.write(key, result);
    }
}
