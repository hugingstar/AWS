import pandas as pd

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

df = pd.read_parquet("/Users/yslee/PycharmProjects/AWS/Data/part-00000-f4f36ddf-6567-40f0-b675-c37753b352c1.c000.snappy.parquet", engine='pyarrow')

print(df.columns)
print(df.head(5))
print(df['uuid'].unique())
print(len(df['uuid'].unique()))

for id in df['uuid'].unique():
    data = df.loc[df['uuid']==id]
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data = data.sort_values('timestamp')
    data.to_csv('Results/{}.csv'.format(id), index=False)
df2 = df[['timestamp', 'Mode', 'Temperatures']]
print(df2)
df2.loc[:6, ['timestamp', 'Temperatures']].to_csv('/Users/yslee/PycharmProjects/AWS/data_check.csv')

df3 = df.Temperatures[0]
print(df3)

df4 = df.Temperatures[0][0]
print(df4)

df5 = df.Temperatures[0][0]['current']
print(df5)