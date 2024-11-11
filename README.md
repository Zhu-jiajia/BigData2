# Question1：根据 user_balance_table 表中的数据，统计所有用户每日的资金流入与流出情况。
读取user_balance_table.csv文件，并挑选出第1、4、8列的数据，分别代表<日期，资金流入量，资金流出量>，并对每日的资金流入量、资金流出量分别加总，得到<日期，当日资金总流入，当日资金总流出>
代码执行：hadoop jar ~/Desktop/Question1-1.0-SNAPSHOT.jar /local/user_balance_table.csv /output/lab2.1

# Question2：统计一周七天中每天的平均资金流入与流出情况，并按照资金流入量从大到小排序。
两次mapreduce，先统计，再排序
代码执行：hadoop jar ~/Desktop/Question2-1.0-SNAPSHOT.jar /local/user_balance_table.csv /output/lab2.2_tmp /output/lab2.2

# Question3：统计每个用户的活跃天数，并按照活跃天数降序排列。
两次mapreduce，先统计，再排序
代码执行：hadoop jar ~/Desktop/Question3-1.0-SNAPSHOT.jar /local/user_balance_table.csv /output/lab2.3_tmp /output/lab2.3

# Question4：交易行为影响因素分析
分析利率对日均资金流动量的影响，导出为excel并进行图的绘制
