package com.question1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Question1_Mapper extends Mapper<Object, Text, Text, Text> {
    private Text dateKey = new Text();
    private Text inflowOutflow = new Text();
    private boolean isFirstLine = true;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }
        if (isFirstLine && line.contains("report_date")) {
            isFirstLine = false;
            return;
        }

        String[] fields = line.split(",");
        // Expected fields: user_id, report_date, tBalance, yBalance,
        // total_purchase_amt, direct_purchase_amt, purchase_bal_amt, purchase_bank_amt,
        // total_redeem_amt
        String reportDate = fields[1].trim();
        String totalPurchaseAmt = fields[4].trim().isEmpty() ? "0" : fields[4].trim();
        String totalRedeemAmt = fields[8].trim().isEmpty() ? "0" : fields[8].trim();

        dateKey.set(reportDate);
        inflowOutflow.set(totalPurchaseAmt + "," + totalRedeemAmt);
        context.write(dateKey, inflowOutflow);
    }
}