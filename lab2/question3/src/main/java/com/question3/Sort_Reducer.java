package com.question3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Sort_Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text userId : values) {
            context.write(userId, key);
        }
    }
}