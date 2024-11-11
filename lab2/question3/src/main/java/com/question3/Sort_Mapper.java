package com.question3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Sort_Mapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable numberKey = new DoubleWritable();
    private Text userValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\t");
        if (parts.length != 2) {
            System.err.println("Invalid input line: " + line);
            return;
        }

        String userId = parts[0];
        String numberString = parts[1];
        double number = 0.0;
        try {
            number = Double.parseDouble(numberString);
        } catch (NumberFormatException e) {
            System.err.println("Invalid number: " + numberString);
            return;
        }

        numberKey.set(number);
        userValue.set(userId);
        context.write(numberKey, userValue);
    }
}