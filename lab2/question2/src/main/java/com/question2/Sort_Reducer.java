package com.question2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Sort_Reducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                outputKey.set(parts[0]);
                outputValue.set(parts[1] + "," + parts[2]);
                context.write(outputKey, outputValue);
            }
        }
    }
}