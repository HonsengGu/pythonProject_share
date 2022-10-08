import numpy as np
import quantstats as qs

class Ghs_Future_Factor:
    def __init__(self, df):
        # df=df.sort_values(by="trade_date", ascending=False)
        self.df= df.sort_values(by="trade_date", ascending=True)
        self.df = df["close"]
        self.trade_date = df.loc[:, "trade_date"]
        self.open = df.loc[ :,"open"]
        self.close = df.loc[ :,"close"]
        self.low = df.loc[ :,"low"]
        self.high = df.loc[ :,"high"]
        self.settle =df.loc[ :,"settle"]
        self.oi =df.loc[ :,"oi"]
        self.vol = df.loc[ :,"vol"]
        self.amount =df.loc[ :,"amount"]
        self.trade_multi=df.loc[:, "合约乘数"]
        self.one_day_pct_change = df.loc[:, "close"].pct_change()

    def mom_factor(self,period):
        df_series=self.close.pct_change(period)
        return df_series

    def volatility_factor(self,period):
        df_series=np.array( self.one_day_pct_change.rolling(period).std()) / np.array(self.one_day_pct_change.rolling(period).mean())
        return df_series

    def skewness_factor(self, period):
        df_series= self.one_day_pct_change.rolling(period).apply( lambda x: qs.stats.skew(x))
        return df_series

    def position_factor(self, period):
        # df_series= np.array((self.oi) * np.array(self.trade_multi.pct_change(period)))
        # df_series = np.array(self.oi.pct_change(period)) * np.array(self.trade_multi)
        df_series = np.array(self.oi.pct_change(period))
        return df_series


# if __name__ == '__main__':
#     data=pd.read_excel(r"C:\Users\39026\Desktop\数据暂存.xlsx")
#     calfactor=Ghs_Future_Factor(data)
#     data["5日动量"]=calfactor.mom_factor(5)
#     print(data)