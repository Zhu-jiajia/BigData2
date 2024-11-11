import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 读取CSV文件并进行日期转换
def load_and_preprocess_data(shibor_file, balance_file):
    # 读取CSV文件
    mfd_bank_shibor_df = pd.read_csv(shibor_file)
    user_balance_table_df = pd.read_csv(balance_file)

    # 转换日期格式
    mfd_bank_shibor_df["mfd_date"] = pd.to_datetime(
        mfd_bank_shibor_df["mfd_date"], format="%Y%m%d"
    )
    user_balance_table_df["report_date"] = pd.to_datetime(
        user_balance_table_df["report_date"], format="%Y%m%d"
    )

    # 合并数据
    return user_balance_table_df.merge(
        mfd_bank_shibor_df, left_on="report_date", right_on="mfd_date", how="inner"
    )


# 计算利率区间并分组
def create_interest_rate_bins(merged_df):
    min_interest = merged_df["Interest_1_W"].min()
    max_interest = merged_df["Interest_1_W"].max()
    interest_bins = np.linspace(min_interest, max_interest, 101)  # 100个区间
    binned_array = np.digitize(merged_df["Interest_1_W"].to_numpy(), interest_bins)

    # 添加分组信息
    merged_df["Interest_1_W_Binned"] = binned_array

    return interest_bins, merged_df


# 分析总购买金额与赎回金额的关系
def analyze_data(merged_df):
    # 根据利率区间分组并计算均值
    result_df = (
        merged_df.groupby("Interest_1_W_Binned")[
            ["total_purchase_amt", "total_redeem_amt"]
        ]
        .mean()
        .reset_index()
    )

    # 添加利率区间标签
    result_df["Interest_Rate_Interval"] = result_df["Interest_1_W_Binned"].apply(
        lambda bin_num: (
            f"{interest_bins[bin_num - 1]:.2f}-{interest_bins[bin_num]:.2f}"
            if bin_num > 0 and bin_num < len(interest_bins)
            else f"{interest_bins[0]:.2f}-{interest_bins[1]:.2f}"
        )
    )

    return result_df


# 绘制图表
def plot_results(result_df):
    # 绘制条形图
    plt.figure(figsize=(20, 8))
    x = np.arange(len(result_df))
    width = 0.35  # 宽度调整
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

    # 图表标题和标签
    plt.xlabel("Interest Rate Interval (%)", fontsize=14)
    plt.ylabel("Amount", fontsize=14)
    plt.title(
        "Total Purchase and Redeem Amounts by Interest Rate Interval (with Time Matching)",
        fontsize=16,
    )

    # 设置X轴标签
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


# 主函数，串联整个流程
def main():
    # 数据加载和预处理
    merged_df = load_and_preprocess_data(
        "mfd_bank_shibor.csv", "user_balance_table.csv"
    )

    # 生成利率区间并分组
    interest_bins, merged_df = create_interest_rate_bins(merged_df)

    # 数据分析
    result_df = analyze_data(merged_df)

    # 输出并保存结果
    result_df.to_excel("result_data.xlsx", index=False)
    print("Excel 文件已成功导出！")

    # 绘制图表
    plot_results(result_df)


# 运行主函数
if __name__ == "__main__":
    main()
