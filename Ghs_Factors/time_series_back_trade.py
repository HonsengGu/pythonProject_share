import talib
import pandas as pd
import numpy as np
import tushare as ts
import matplotlib.pyplot as plt
import datetime

class backtest_days:
    def __init__(self, TimestampSignal, price_df, opencost, closecost):
        self.TimestampSignal = TimestampSignal   #信号时间序列
        self.price_df = price_df   #价格时间序列
        self.opencost = opencost  #开仓成本
        self.closecost = closecost  #平仓成本

    def long(self, openprice, closeprice, opencost, closecost):
        return ((closeprice * (1 - closecost)) / (openprice * (1 + opencost)) - 1)  #平仓价（扣除成本）/开仓价（扣除成本）-1

    def short(self, openprice, closeprice, opencost, closecost):
        nominator = openprice * (1 - opencost) - closeprice * (1 + closecost)
        denominator = openprice * (1 - opencost)
        return (nominator / denominator)

    def trading_index_finder(self, actions):
        if len(actions) == 1:
            actionindex1 = 'end'
        else:
            actionindex1 = 0
        try:
            if (actions.iloc[actionindex1] == 1):
                actionindex2 = np.where(actions == 0)[0]
                actionindex2 = actionindex2[actionindex2 > actionindex1][0]
            if (actions.iloc[actionindex1] == 0):
                actionindex2 = np.where(actions == 1)[0]
                actionindex2 = actionindex2[actionindex2 > actionindex1][0]
        except:
            actionindex2 = 'end'
        return [actionindex1, actionindex2]

    def crossday_backtest_everyday(self):

        date_return_list = [[self.TimestampSignal['Date'].iloc[0], 0]]
        TimestampSignal = self.TimestampSignal

        while (len(TimestampSignal) > 0):
            #只要有信号一直回测一直交易
            [actionindex1, actionindex2] = self.trading_index_finder(TimestampSignal['Prediction'])
            #找出开仓和平仓的index

            if (actionindex1 == 'end'):
                break
            if (actionindex2 == 'end'):
                [Date1, Action1] = TimestampSignal[['Date', 'Prediction']].iloc[actionindex1]
                [Date2, Action2] = TimestampSignal[['Date', 'Prediction']].iloc[-1]

            if ((actionindex1 != 'end') & (actionindex2 != 'end')):
                [Date1, Action1] = TimestampSignal[['Date', 'Prediction']].iloc[actionindex1]
                [Date2, Action2] = TimestampSignal[['Date', 'Prediction']].iloc[actionindex2]

            #根据date1和date2找到相对应的日期和价格
            DTtradep = self.price_df[(self.price_df['Date'] >= Date1) & (self.price_df['Date'] <= Date2)]
            print(Date1,Date2)
            if (Action1 == 1):
                payoff = self.long(DTtradep['close'].iloc[0], DTtradep['close'].iloc[-1], self.opencost, self.closecost)
                date_return_list.append([DTtradep['Date'].iloc[-1], payoff])

            if (Action1 == (0)):
                payoff = self.short(DTtradep['close'].iloc[0], DTtradep['close'].iloc[-1], self.opencost, self.closecost)
                date_return_list.append([DTtradep['Date'].iloc[-1], payoff])
            # print()
            #更新交易信号对，把已经有的信号删除
            TimestampSignal = TimestampSignal[TimestampSignal['Date'] >= DTtradep['Date'].iloc[-1]]

        return date_return_list

def SearchForBestCoef(price_df,coef):   #寻找最优参数
    valuemax=0
    for m in coef:
        for n in coef:
            if m<n:
                ShortSMA=talib.SMA(price_df['close'].values,m)
                LongSMA=talib.SMA(price_df['close'].values,n)
                TradingSignal=(ShortSMA>LongSMA).astype(int)  #默认NaN>NaN是0   #短期均线大于长期均线为1
                price_df['Prediction']=TradingSignal
                TimestampSignal=price_df[['Date','Prediction']]
                TimestampSignal=TimestampSignal.sort_values(by='Date')
                TimestampSignal=TimestampSignal.reset_index(drop=True)
                backtestresult=backtest_days(TimestampSignal,price_df,opencost=3/10000,closecost=3/10000)  #回测实例化
                date_return_list=backtestresult.crossday_backtest_everyday()
                date_return_list=pd.DataFrame(date_return_list)
                date_return_list.columns=['Date','Return']
                if date_return_list['Return'].sum()>valuemax:
                    valuemax=date_return_list['Return'].sum()
                    coefmax=[m,n]
    return coefmax

if __name__ == '__main__':

    price_df = pd.read_pickle(r'C:\Users\39026\Desktop\price_df.pickle')

    price_df['Date'] = price_df.index
    price_df['Date'] = [x.date() for x in price_df['Date']]
    price_df = price_df[price_df['Date'] >= datetime.date(2010,4,19)]
    
    price_df = price_df.sort_values(by='Date')
    price_df = price_df.reset_index(drop=True)
    price_df = price_df[price_df['Date'] < datetime.date(2012,6,30)]

    opencost = 3 / 10000   #手续费+滑点
    closecost = 3 / 10000   #手续费+滑点
    # coef = [2, 3, 5, 8, 13, 21, 34, 55, 89, 144]  #参数优化范围
    trade_signal_list = []

    for each_tradeday in price_df['Date']:

        if each_tradeday > datetime.date(2010,12,30):

            InSample = price_df[price_df['Date'] < each_tradeday]
            [m, n]= [20, 60]   #均线交易的参数
            OutofSample = price_df[price_df['Date'] <= each_tradeday]
            ShortSMA = talib.SMA(OutofSample['close'].values, m)
            LongSMA = talib.SMA(OutofSample['close'].values, n)

            if ShortSMA[-1] > LongSMA[-1]:
                trade_signal_list.append([each_tradeday, 1])
            else:
                trade_signal_list.append([each_tradeday, 0])

    trade_signal_list = pd.DataFrame(trade_signal_list)
    trade_signal_list.columns = ['Date', 'Prediction']

    backtestresult = backtest_days(trade_signal_list, price_df, opencost=3 / 10000, closecost=3 / 10000) # 回测实例化

    date_return_list = backtestresult.crossday_backtest_everyday()

    date_return_list = pd.DataFrame(date_return_list)
    date_return_list.columns = ['Date', 'Return']
    date_return_list['Value'] = date_return_list['Return'].cumsum()
    date_return_list.set_index(['Date'], inplace=True)
    trade_signal_list.to_excel("信号.xlsx")

    plt.figure()
    date_return_list['Value'].plot()
    plt.show()
