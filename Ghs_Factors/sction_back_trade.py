import math
import tushare as ts
import pandas as pd
from pymongo import MongoClient, ASCENDING
import numpy as np
import datetime
import json
pro = ts.pro_api("ab9bd05c38852f579af7c43978e914a93e242e19e760dedb310ed827")

class Ghs_Future_Section_Back_Trade:
    def __init__(self, db_name):
        self.start_day = "20150101"
        self.select_day = datetime.datetime.now().strftime('%Y-%m-%d')
        self.end_day = ''.join(filter(str.isalnum,self.select_day ))
        self.factor_dflonglist = []
        self.factor_dfshortlist = []
        self.date_df_long_list = []
        self.df_future_concat = pd.DataFrame()
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.future_code_total =  self.db .list_collection_names(session=None)
        self.future_code_total = [x for x in self.future_code_total if x not in ["ZC.ZCE", "PF.ZCE", "BB.DCE", "SF.ZCE", "WH.ZCE"]]

    def section_back_trades(self, factor_name,rank_ascend=True):
        self.client = MongoClient('localhost', 27017)
        df_future_concat = self.df_future_concat.reset_index()

        for code in self.future_code_total:
            table =  self.db [code]
            table_df = pd.DataFrame(list(table.find())).loc[:, ["trade_date", "name", "1_days_pct_change", "{}_{}".format("5", factor_name),
                                                                "{}_{}".format("22", factor_name), "{}_{}".format("66", factor_name), "{}_{}".format("126",
                                                                factor_name), "{}_{}".format("252", factor_name),]]

            table_df = table_df[(table_df["trade_date"] <= self. end_day ) & (table_df["trade_date"] >= self.start_day )]
            table_df.loc[:, "shift_earn"] = table_df.loc[:, "1_days_pct_change"].shift(-1)
            table_df.loc[:, "trade_date"] = pd.to_datetime(table_df.loc[:, "trade_date"])
            df_future_concat =  df_future_concat .append(table_df)
        df_future_concat = df_future_concat.reset_index()

        df_future_concat =  df_future_concat.set_index(["name", "trade_date"]).unstack(["trade_date"])


        # df_future_concat.to_excel("{}.csv".format(factor_name))
        db_factor =self.client['{}收益率数据'.format(factor_name)]
        db_postion =self.client['{}持仓数据'.format(factor_name)]

        for period in ["{}_{}".format("5", factor_name), "{}_{}".format("22", factor_name),
                       "{}_{}".format("66", factor_name), "{}_{}".format("126", factor_name),
                       "{}_{}".format("252", factor_name),]:

            each_day_info = pd.DataFrame()
            table_to_save = db_factor[period]
            table_to_save_postion = db_postion[period]

            df_future_concat.dropna(axis=1, thresh=0.6 * len(df_future_concat), inplace=True)
            mot_ndays = df_future_concat.loc[:, (period, slice(None))]

            date_list = mot_ndays.columns.values.tolist()
            date_list = [x[1] for x in date_list]

            for date in date_list:
                factor_list = df_future_concat.loc[:, (period, date)].sort_values(ascending=rank_ascend).dropna()

                len_of_list = math.ceil(len(factor_list) * 0.2)

                long_position = factor_list.head(len_of_list).index.values.tolist()
                short_position = factor_list.tail(len_of_list).index.values.tolist()

                for long_future_name, short_future_name in zip(long_position, short_position):
                    try:
                        daily_earn = df_future_concat.loc[long_future_name, ("shift_earn", date)]
                        daily_lost = df_future_concat.loc[short_future_name, ("shift_earn", date)]
                    except:
                        daily_earn = np.nan
                        daily_lost = np.nan
                    each_day_info = each_day_info.append({"日期": date, "多头": daily_earn, "多头品种": long_future_name, "空头": daily_lost, "空头品种": short_future_name}, ignore_index=True, )

            each_day_info.dropna(inplace=True)

            long_temp = each_day_info.loc[:, ["日期", "多头", "多头品种"]].reset_index(drop=True).dropna(axis=0, how="any").drop_duplicates(subset=["日期", "多头品种"], keep="first")
            short_temp = each_day_info.loc[:, ["日期", "空头", "空头品种"]].reset_index(drop=True).dropna(axis=0, how="any").drop_duplicates(subset=["日期", "空头品种"], keep="first")

            each_day_long_info = long_temp.set_index(["日期", "多头品种"]).unstack(["日期"])
            each_day_short_info = short_temp.set_index(["日期", "空头品种"]).unstack(["日期"])

            sum_long_dataframe = pd.DataFrame(each_day_long_info.mean())
            sum_short_dataframe = pd.DataFrame(each_day_short_info.mean())

            long_and_short = pd.merge(sum_long_dataframe, sum_short_dataframe, on="日期", how="left")
            long_and_short.rename(columns={"0_x": "多头总收益", "0_y": "空头总收益"}, inplace=True)
            long_and_short.loc[:, "多空合计收益率"] = long_and_short.loc[:, "多头总收益"] - long_and_short.loc[:, "空头总收益"]
            long_and_short.reset_index(inplace=True)
            long_and_short.loc[:, "日期"] = long_and_short.loc[:, "日期"].apply(lambda x: x.strftime("%Y%m%d"))

            each_day_info.to_excel("each_day_info.xlsx")

            try:
                table_to_save.drop()
            except:
                pass
            table_to_save.insert_many(json.loads(long_and_short.T.to_json()).values())
            try:
                each_day_info.loc[:, "日期"] = each_day_info.loc[:, "日期"].apply(lambda x: x.strftime("%Y%m%d"))
            except:
                pass
            try:
                table_to_save_postion.drop()
            except:
                pass
            table_to_save_postion.insert_many(json.loads(each_day_info.T.to_json()).values())
        self.client.close()


if __name__ == '__main__':
    back_trade=Ghs_Future_Section_Back_Trade("期货日收益率数据")
    # back_trade.section_back_trades(factor_name="滚动偏度因子",rank_ascend=True)
    # back_trade.section_back_trades(factor_name="滚动波动因子",rank_ascend=False)
    back_trade.section_back_trades(factor_name="滚动动量因子",rank_ascend=False)
    # back_trade.section_back_trades(factor_name="滚动持仓因子",rank_ascend=True)

#     data=pd.read_excel(r"C:\Users\39026\Desktop\数据暂存.xlsx")
#     calfactor=Ghs_Future_Factor(data)
#     data["5日动量"]=calfactor.mom_factor(5)
#     print(data)