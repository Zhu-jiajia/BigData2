package com.question2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Question2_Mapper extends Mapper<Object, Text, Text, Text> {
    // 需要输出的key和value对象
    private Text dayOfWeek = new Text();
    private Text inflowOutflowData = new Text();

    // 判断是否为文件的首行
    private boolean isHeader = true;

    // 用于解析日期格式
    private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

    /**
     * map方法
     * 输入：每一行的数据
     * 输出：基于星期几分类的资金流入和流出信息
     */
    @Override
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        String row = value.toString();

        // 检查空行
        if (row == null || row.isEmpty()) {
            return;
        }

        // 跳过首行（表头）
        if (isHeader && row.contains("report_date")) {
            isHeader = false;
            return;
        }

        // 将每行数据分割为字段
        String[] columns = row.split(",");

        // 确保每行有足够的字段
        if (columns.length < 9) {
            System.err.println("Invalid row (missing fields): " + row);
            return;
        }

        try {
            // 获取并清理数据：购买金额和赎回金额
            String purchaseAmount = columns[4].trim();
            String redeemAmount = columns[8].trim();

            // 如果字段为空，赋值为0
            if (purchaseAmount.isEmpty()) {
                purchaseAmount = "0";
            }
            if (redeemAmount.isEmpty()) {
                redeemAmount = "0";
            }

            // 解析日期字段，计算星期几
            Date reportDate = inputDateFormat.parse(columns[1].trim());
            SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);
            String weekday = dayFormat.format(reportDate);

            // 转换日期为yyyyMMdd格式字符串
            String formattedDate = inputDateFormat.format(reportDate);

            // 设置输出的key和value
            dayOfWeek.set(weekday);
            inflowOutflowData.set(formattedDate + "," + purchaseAmount + "," + redeemAmount);

            // 将结果写入context，按星期几分组
            context.write(dayOfWeek, inflowOutflowData);

        } catch (ParseException e) {
            // 处理日期解析异常
            System.err.println("Date parsing error in row: " + row);
            e.printStackTrace();
        } catch (NumberFormatException e) {
            // 处理数字格式异常
            System.err.println("Number parsing error in row: " + row);
            e.printStackTrace();
        }
    }
}
