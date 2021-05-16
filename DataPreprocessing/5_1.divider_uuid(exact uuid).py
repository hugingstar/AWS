import pandas as pd
import glob
import os
"""
이 코드는 하나의 디바이스 타입 중에서 하나의 uuid 만을 정리하기 위한 코드임.
즉, 이 코드를 돌리기 위해서는 descript, uuid를 입력해 주어야 한다.
"""
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함."""
Dic1 = 'devicetype=Air-Conditioner(descript)'
Dic2 = 'devicetype=Air-Conditioner(uuid)'

data = pd.DataFrame()
descript = '4WAY'
uuid = '702C1F53-FCDF-0000-0000-000000000000_032000000' #보고싶은 uuid를 선택적으로 뽑을려면

try:
    if not os.path.exists(Dic2 + '/' + descript):
        """description 없으면 디렉토리 생성"""
        os.makedirs(Dic2 + '/' + descript)
    else:
        pass
except OSError:
    print('Error: Creating directory.')

# files = glob.glob('*.csv')
files = glob.glob(Dic1 + '/' + descript + '/' + '*.parquet')
print(files)

total = pd.DataFrame()
for file in files:
    print(file)
    df = pd.read_parquet(file, engine='pyarrow')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    data = df[df['uuid'] == uuid]

    if not len(data) == 0:
        data = data.reset_index(drop=True)

        temp = []
        for i in data.index:
            if type(data.loc[i, 'tags']) == str:
                tag = data.loc[i, 'tags'].replace(' ', ',')
            else:
                tag = data.loc[i, 'tags']

            resource = str(data.loc[i, 'resourceName'])
            if type(tag) == str:
                if (len(eval(tag))) > 1:
                    print(data.loc[int(i), resource])
                    if pd.isnull(data.loc[int(i), resource]) == True:
                        d = {}
                    elif data.loc[int(i), resource][0] == '[':
                        d = data.loc[int(i), resource].replace('{', ',{')
                        d = d.replace("[,{", "[{")
                        # d = d.replace("[", "")
                        # d = d.replace("]", "")
                        if int(len(eval(d))) <= 0:
                            d = {}
                        else:
                            d = eval(d)[0]
                    else:
                        if 'array' in data.loc[i, resource]:
                            if pd.isnull(data.loc[i, resource]) == True:
                                d = {}
                            else:
                                d = data.loc[i, resource].replace('array(', '')
                                d = d.replace(', dtype=object)', '').replace('dtype=object)', '').replace(' ,', '')
                                d = eval(d)

                elif pd.isnull(data.loc[int(i), resource]) == True:
                    d = {}
                elif 'array' in data.loc[i, resource]:
                    if pd.isnull(data.loc[i, resource]) == True:
                        d = {}
                    else:
                        d = data.loc[i, resource].replace('array(', '')
                        d = d.replace(', dtype=object)', '').replace('dtype=object)', '').replace(' ,', '')
                        d = eval(d)
                else:
                    if pd.isnull(data.loc[i, resource]) == True:
                        d = {}
                    else:
                        print(data.loc[i, resource])
                        print(type(data.loc[i, resource]))
                        d = eval(data.loc[i, resource])
            else:
                print(data.loc[i, resource])
                print(type(data.loc[int(i), resource]))
                """처음 값이 '[' 이면"""
                if pd.isnull(data.loc[int(i), resource]) == True:
                    d = {}
                else:
                    if len(data.loc[int(i), resource]) == 0:
                        d = {}
                    else:
                        if type(data.loc[int(i), resource])==dict:
                            d = data.loc[i, resource]
                        else:
                            d = data.loc[int(i), resource][0]

            d = {'{}({})'.format(resource, k): v for k, v in d.items()}
            d['timestamp'] = data.loc[i, 'timestamp']
            temp.append(d)
            del d
        temp = pd.DataFrame(temp)
        print(temp)
        print(data)
        data = pd.merge(data, temp, how='outer', on='timestamp')
        del temp
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data = data.sort_values('timestamp')
        total = pd.concat([total, data])
        total.index = pd.to_datetime(total['eventTime'])
        # del total['eventTime']
        # del total['timestamp']
        total = total.sort_index()
        total.to_csv(Dic2 + '/' + descript + '/' + '{}_{}.csv'.format(descript, uuid))







