package com.question2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question2_Manager {
    public static void main(String[] args) throws Exception {
        // 检查命令行参数的有效性
        if (args.length != 3) {
            System.err.println("Usage: Question2_Manager <input path> <temporary output path> <final output path>");
            System.exit(-1);
        }

        // 配置第一个MapReduce任务：计算每个星期几的资金分析
        Configuration config1 = new Configuration();
        Job job1 = Job.getInstance(config1, "Question2_Manager - Step 1: Weekday Calculation");
        job1.setJarByClass(Question2_Manager.class);
        job1.setMapperClass(Question2_Mapper.class); // 使用自定义的Mapper类
        job1.setReducerClass(Question2_Reducer.class); // 使用自定义的Reducer类
        job1.setMapOutputKeyClass(Text.class); // 设置Mapper输出的Key类型
        job1.setMapOutputValueClass(Text.class); // 设置Mapper输出的Value类型
        job1.setOutputKeyClass(Text.class); // 设置Reducer输出的Key类型
        job1.setOutputValueClass(Text.class); // 设置Reducer输出的Value类型
        FileInputFormat.addInputPath(job1, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); // 临时输出路径

        // 等待第一个任务执行完成
        boolean firstJobSuccess = job1.waitForCompletion(true);
        if (!firstJobSuccess) {
            System.exit(1); // 如果第一个任务失败，则退出
        }

        // 配置第二个MapReduce任务：对结果进行排序
        Configuration config2 = new Configuration();
        Job job2 = Job.getInstance(config2, "Question2_Manager - Step 2: Sorting Results");
        job2.setJarByClass(Question2_Manager.class);
        job2.setMapperClass(Sort_Mapper.class); // 使用自定义的排序Mapper类
        job2.setReducerClass(Sort_Reducer.class); // 使用自定义的排序Reducer类
        job2.setMapOutputKeyClass(DoubleWritable.class); // 设置Mapper输出的Key类型为DoubleWritable
        job2.setMapOutputValueClass(Text.class); // 设置Mapper输出的Value类型为Text
        job2.setOutputKeyClass(Text.class); // 设置Reducer输出的Key类型
        job2.setOutputValueClass(Text.class); // 设置Reducer输出的Value类型
        job2.setSortComparatorClass(DescendingComparator.class); // 设置自定义的排序比较器（降序排序）
        FileInputFormat.addInputPath(job2, new Path(args[1])); // 临时结果路径作为输入
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // 最终结果输出路径

        // 等待第二个任务执行完成
        boolean secondJobSuccess = job2.waitForCompletion(true);
        if (!secondJobSuccess) {
            System.exit(1); // 如果第二个任务失败，则退出
        }

        // 程序成功结束
        System.exit(0);
    }
}
