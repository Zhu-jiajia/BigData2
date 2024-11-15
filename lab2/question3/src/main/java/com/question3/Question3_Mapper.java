package com.question3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Question3_Mapper extends Mapper<Object, Text, Text, Text> {
    private Text userIdKey = new Text();
    private Text inflowOutflow = new Text();
    private boolean isFirstLine = true;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) {
            return;
        }
        if (isFirstLine && line.contains("report_date")) {
            isFirstLine = false;
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 9) {
            System.err.println("Invalid input line (not enough fields): " + line);
            return;
        }
        // user_id,report_date,tBalance,yBalance,total_purchase_amt,direct_purchase_amt,purchase_bal_amt,purchase_bank_amt,total_redeem_amt
        String userId = fields[0].trim();
        String reportDate = fields[1].trim();
        String totalPurchaseAmt = fields[5].trim().isEmpty() ? "0" : fields[5].trim();
        String totalRedeemAmt = fields[8].trim().isEmpty() ? "0" : fields[8].trim();
        String isActive = (!totalPurchaseAmt.equals("0") || !totalRedeemAmt.equals("0")) ? "1" : "0";

        userIdKey.set(userId);
        inflowOutflow.set(reportDate + "," + isActive);
        context.write(userIdKey, inflowOutflow);
    }
}
