import pandas as pd

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""Dic : devicetype=Air_Conditioner"""
file_path = "/Users/yslee/PycharmProjects/AWS/Data/AWS_Sample/"
df1 = pd.read_parquet(file_path+"devicetype=Air_Conditioner/date=2020-04-07/part-00000-dc77eebb-9ff8-4d2f-9cb5-1ef1e75f67cc.c000.snappy.parquet", engine='pyarrow')
print(df1.columns)
print("\n", df1.head(10))

print(df1.columns)
print(df1.head(5))
print(df1['uuid'].unique())
print(len(df1['uuid'].unique()))
save_path = "/Users/yslee/PycharmProjects/AWS/Results/"
for uuid in df1['uuid'].unique():
    data = df1.loc[df1['uuid']==uuid]
    data['eventTime'] = pd.to_datetime(data['eventTime'])
    data = data.sort_values('eventTime')
    print(data)
    data.to_csv(save_path+'devicetype=Air_Conditioner/date=2020-04-07/uuid_{}.csv'.format(uuid), index=False)
