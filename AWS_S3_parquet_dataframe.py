import pandas as pd
import numpy as np
import ast
file_path = "/Users/yslee/PycharmProjects/AWS/Results/devicetype=Air_Conditioner/date=2020-04-07/uuid_0D2389F5-2F97-9D89-F0F4-1875F577F2E5_0D2389F5-2F97-9D89-F0F4-000001200200.csv"

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)


df = pd.read_csv(file_path)
# df2 =df[['EnergyConsumption', 'Temperatures']]
# df2_list = df2.Temperatures
# print(ast.literal_eval(df2_list))
tf=[]
te=[]
for i in df.index:
   t = df.loc[i, 'Temperatures']
   e = df.loc[i, 'EnergyConsumption']

   if pd.isnull(t)==False:
      tt = ast.literal_eval(t)[0]
      tt['timestamp'] = df.loc[i, 'timestamp']
      tf.append(tt)
   if pd.isnull(e)==False:
      et = ast.literal_eval(e)
      et['timestamp'] = df.loc[i, 'timestamp']
      te.append(et)
df3 = pd.DataFrame(tf)
df3 = df3.rename(columns=lambda x: 'Temperature_'+x if x!='timestamp' else x)
df4 = pd.DataFrame(te)
df4 = df4.rename(columns=lambda x: 'Energy_'+x if x!='timestamp' else x)
print(df3)
print(df4)

data = pd.merge(df3, df4, how='outer', on='timestamp')
print(data)
data = data.dropna(how='all', axis=1)
data.index = pd.to_datetime(data['timestamp'])
del data['timestamp']
data = data.sort_index()
data.to_csv("/Users/yslee/PycharmProjects/AWS/Results/molla.csv")