import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 读取CSV文件
mfd_bank_shibor_df = pd.read_csv("mfd_bank_shibor.csv")
user_balance_table_df = pd.read_csv("user_balance_table.csv")

# 转换日期格式
mfd_bank_shibor_df["mfd_date"] = pd.to_datetime(
    mfd_bank_shibor_df["mfd_date"], format="%Y%m%d"
)
user_balance_table_df["report_date"] = pd.to_datetime(
    user_balance_table_df["report_date"], format="%Y%m%d"
)

# 合并数据
merged_df = user_balance_table_df.merge(
    mfd_bank_shibor_df, left_on="report_date", right_on="mfd_date", how="inner"
)

# 计算利率的最小值和最大值
min_interest = merged_df["Interest_1_W"].min()
max_interest = merged_df["Interest_1_W"].max()

# 划分利率区间
interest_bins = np.linspace(min_interest, max_interest, 101)
interest_array = merged_df["Interest_1_W"].to_numpy()
binned_array = np.digitize(interest_array, interest_bins)

# 将分组结果添加到DataFrame
merged_df["Interest_1_W_Binned"] = binned_array

# 根据利率区间分组并计算均值
result_df = (
    merged_df.groupby("Interest_1_W_Binned")[["total_purchase_amt", "total_redeem_amt"]]
    .mean()
    .reset_index()
)

# 输出分组结果
print(result_df)

result_df.to_excel("result_data.xlsx", index=False)
print("Excel 文件已成功导出！")

# 转换为pandas DataFrame并生成利率区间标签
result_df["Interest_Rate_Interval"] = result_df["Interest_1_W_Binned"].apply(
    lambda bin_num: (
        f"{interest_bins[bin_num - 1]:.2f}-{interest_bins[bin_num]:.2f}"
        if bin_num > 0 and bin_num < len(interest_bins)
        else f"{interest_bins[0]:.2f}-{interest_bins[1]:.2f}"
    )
)

# 绘制条形图
plt.figure(figsize=(20, 8))
x = np.arange(len(result_df))
width = 0.2
plt.bar(
    x - width / 2,
    result_df["total_purchase_amt"],
    width=width,
    label="Total Purchase Amount",
    alpha=0.7,
)
plt.bar(
    x + width / 2,
    result_df["total_redeem_amt"],
    width=width,
    label="Total Redeem Amount",
    alpha=0.7,
)
plt.xlabel("Interest Rate Interval (%)", fontsize=14)
plt.ylabel("Amount", fontsize=14)
plt.title(
    "Total Purchase and Redeem Amounts by Interest Rate Interval (with Time Matching)",
    fontsize=16,
)
plt.xticks(
    ticks=x,
    labels=result_df["Interest_Rate_Interval"],
    rotation=90,
    ha="right",
    fontsize=10,
)
plt.legend(fontsize=12)
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
