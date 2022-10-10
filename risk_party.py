from __future__ import division
import tushare as ts
import riskfolio as rp
import datetime
from pymongo import MongoClient, ASCENDING
import numpy as np
# -*- coding: gbk -*-
import empyrical as em
import pyfolio
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

pro = ts.pro_api("ab9bd05c38852f579af7c43978e914a93e242e19e760dedb310ed827")


def get_ready():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    plt.rcParams['font.sans-serif'] = ['KaiTi']  # 指定默认字体
    plt.rcParams['axes.unicode_minus'] = False


def get_select_df(select_group, start_day, end_day):
    client = MongoClient('localhost', 27017)
    db = client['XXX子基金净值汇总']
    table = db['Df_Mongo']
    fund_total = pd.DataFrame(columns=["日期"])
    for name in select_group:
        select_fund = pd.DataFrame(list(table.find({'产品名称': name})))[["产品名称", "日期", "累计单位净值"]]
        select_fund = select_fund.drop_duplicates(subset=['日期'], keep='first')
        select_fund.loc[:, name] = select_fund.loc[:, "累计单位净值"].pct_change(1)
        fund_total = pd.merge(fund_total, select_fund.loc[:, ["日期", name]], on="日期", how='outer')
    fund_total = fund_total.sort_values(by="日期", ascending=True).dropna(axis=0).reset_index(drop=True)
    fund_total.loc[:, "日期"] = pd.to_datetime(fund_total.loc[:, "日期"], format='%Y%m%d')
    fund_total = fund_total.loc[(fund_total.loc[:, "日期"] >= start_day) & (fund_total.loc[:, "日期"] <= end_day), :]

    fund_total = fund_total.set_index(["日期"])
    return fund_total


def out_pic(pct_serise, plt_title):
    pnl = pd.Series(pct_serise)
    cumulative = em.cum_returns(pnl, starting_value=1)
    max_return = cumulative.cummax()
    drawdown = (cumulative - max_return) / max_return
    perf_stats_year = (pnl).groupby(pnl.index.to_period('y')).apply(
        lambda data: pyfolio.timeseries.perf_stats(data)).unstack()
    perf_stats_all = pyfolio.timeseries.perf_stats((pnl)).to_frame(name='all')
    perf_stats = pd.concat([perf_stats_year, perf_stats_all.T], axis=0)
    perf_stats_ = round(perf_stats, 4).reset_index()
    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [1.5, 4]}, figsize=(20, 8))
    cols_names = ['date', 'Annual\nreturn', 'Cumulative\nreturns', 'Annual\nvolatility',
                  'Sharpe\nratio', 'Calmar\nratio', 'Stability', 'Max\ndrawdown',
                  'Omega\nratio', 'Sortino\nratio', 'Skew', 'Kurtosis', 'Tail\nratio',
                  'Daily value\nat risk']
    # 绘制表格
    ax0.set_axis_off()  # 除去坐标轴
    table = ax0.table(cellText=perf_stats_.values,
                      bbox=(0, 0, 1, 1),  # 设置表格位置， (x0, y0, width, height)
                      rowLoc='right',  # 行标题居中
                      cellLoc='right',
                      colLabels=cols_names,  # 设置列标题
                      colLoc='right',  # 列标题居中
                      edges='open'  # 不显示表格边框
                      )
    table.set_fontsize(13)
    ax2 = ax1.twinx()
    ax1.set_title(plt_title, fontsize=20)

    ax1.yaxis.set_ticks_position('right')  # 将回撤曲线的 y 轴移至右侧
    ax2.yaxis.set_ticks_position('left')  # 将累计收益曲线的 y 轴移至左侧
    drawdown.plot.area(ax=ax1, label='drawdown (right)', rot=0, alpha=0.3, fontsize=13, grid=False)
    (cumulative).plot(ax=ax2, color='#F1C40F', lw=3.0, label='cumret (left)', rot=0, fontsize=13, grid=False)
    ax2.set_xbound(lower=cumulative.index.min(), upper=cumulative.index.max())
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(int(len(cumulative) / 5)))
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    plt.legend(h1 + h2, l1 + l2, fontsize=12, loc='upper left', ncol=1)
    fig.tight_layout()  # 规整排版
    plt.show()


def back_test(fund_total):
    w_df = pd.DataFrame()
    fund_concact = pd.DataFrame()
    for i in range(0, len(fund_total), period):

        fund_rolling = fund_total.iloc[0:(i + period), :]
        fund_forward = fund_total.iloc[i + period:(i + 2 * period), :]
        a = np.array(fund_forward)

        port = rp.Portfolio(returns=fund_rolling)
        port.assets_stats(method_mu='hist', method_cov='hist')
        w_rp_MV = port.rp_optimization(model='Classic', rm='MV', rf=0, hist=True)

        try:
            w_rp_MV.rename(columns={'weights': fund_forward.index[0]}, inplace=True)
        except:
            pass

        w_df = w_df.append(w_rp_MV.T)
        b = np.array(w_rp_MV)
        c = np.dot(a, b)

        fund_forward.loc[:, "等权日收益"] = fund_forward.mean(axis=1)
        fund_forward.loc[:, "拟合日收益"] = c
        fund_concact = pd.concat([fund_concact, fund_forward], axis=0)

    w_df.columns = [x + "权重" for x in w_df.columns]

    fund_concact.loc[:, "等权净值"] = em.cum_returns(fund_concact.loc[:, "等权日收益"], starting_value=1)
    fund_concact.loc[:, "拟合净值"] = em.cum_returns(fund_concact.loc[:, "拟合日收益"], starting_value=1)

    # 按照左边索引合并
    fund_concact = pd.merge(fund_concact, w_df, left_index=True, right_index=True, how='left')
    fund_total = pd.merge(fund_total, w_df, left_index=True, right_index=True, how='left')

    fund_total.to_excel("原始日收益数据数据.xlsx")
    fund_concact.to_excel("拟合日收益数据.xlsx")

    out_pic(fund_concact.loc[:, "拟合日收益"], plt_title="风险平价拟合累计净值")
    out_pic(fund_concact.loc[:, "等权日收益"], plt_title="等权累计净值")


if __name__ == '__main__':
    period = 44
    select_group = ["A产品", "B产品", "C产品", "D产品", "E产品", "F产品"]
    
    start_day = datetime.datetime.strptime("20211212", "%Y%m%d");
    end_day = datetime.datetime.strptime("20220701", "%Y%m%d")

    get_ready()

    fund_total = get_select_df(select_group, start_day, end_day)

    back_test(fund_total)



