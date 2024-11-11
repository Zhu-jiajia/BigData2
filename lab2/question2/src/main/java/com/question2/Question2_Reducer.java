package com.question2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Question2_Reducer extends Reducer<Text, Text, Text, Text> {
    private Text output = new Text();

    /**
     * Reducer的reduce方法
     * 
     * @param key     输入的key，通常是日期或其他标识
     * @param values  输入的values，包含每个日期的多个交易记录
     * @param context Hadoop的上下文，用于输出结果
     * @throws IOException          如果写入输出时出现问题
     * @throws InterruptedException 如果任务中断
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 初始化变量，存储数据流入和流出的总金额
        double totalInflow = 0.0;
        double totalOutflow = 0.0;

        // 使用HashSet来存储不重复的日期
        Set<String> datesSet = new HashSet<>();

        // 使用HashMap来记录每个日期的流入和流出金额
        Map<String, Double[]> dateTransactionMap = new HashMap<>();

        // 遍历传入的所有交易记录
        for (Text value : values) {
            // 拆分每一条记录，期望格式为 "date,inflow,outflow"
            String[] transactionParts = value.toString().split(",");
            if (transactionParts.length < 3) {
                // 如果格式不正确，输出错误信息并跳过此记录
                System.err.println("Skipping invalid record (missing parts): " + value.toString());
                continue;
            }

            try {
                // 提取日期、流入金额和流出金额
                String date = transactionParts[0];
                double inflow = Double.parseDouble(transactionParts[1]);
                double outflow = Double.parseDouble(transactionParts[2]);

                // 将日期添加到不重复日期集合中
                datesSet.add(date);

                // 如果该日期已经存在于Map中，累加流入和流出的金额
                if (dateTransactionMap.containsKey(date)) {
                    Double[] amounts = dateTransactionMap.get(date);
                    amounts[0] += inflow;
                    amounts[1] += outflow;
                } else {
                    // 如果该日期不在Map中，则初始化该日期的流入和流出金额
                    dateTransactionMap.put(date, new Double[] { inflow, outflow });
                }
            } catch (NumberFormatException e) {
                // 如果解析数字时发生异常，记录错误并跳过此条记录
                System.err.println("Error parsing number in record: " + value.toString());
                e.printStackTrace();
                continue;
            }
        }

        // 获取唯一日期的数量
        int uniqueDatesCount = datesSet.size();
        if (uniqueDatesCount == 0) {
            // 如果没有有效的日期，则输出错误日志并返回
            System.err.println("No valid dates found for key: " + key.toString());
            return;
        }

        // 计算所有日期的流入和流出金额的总和
        for (Double[] amounts : dateTransactionMap.values()) {
            totalInflow += amounts[0];
            totalOutflow += amounts[1];
        }

        // 计算平均流入和平均流出
        double averageInflow = totalInflow / uniqueDatesCount;
        double averageOutflow = totalOutflow / uniqueDatesCount;

        // 设置输出结果格式，包含平均流入和平均流出
        output.set(averageInflow + "," + averageOutflow);

        // 将结果写入上下文，输出key和对应的值
        context.write(key, output);
    }
}
