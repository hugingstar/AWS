import pandas as pd
import glob
import os
import json
import pyarrow.parquet as pq
import gc
import datetime
import pyarrow
"""
이 코드는 디바이스 타입(e.g. 1WAY)별로 모든 uuid가 들어있는 덩어리로 나누어진 파일을
사용할 때 너무 크니까 작게 쪼게서 사용할 수 있도록 하는 파일로 5 덩어리로 파일을 자를 수 있도록
코딩되어 있음. 
"""
"""
가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함.
이 코딩의 출력 결과는 5덩어리로 쪼게진 디바이스 타입별 데이터 파일
"""
Dic1 = 'devicetype=Air-Conditioner'
"""
Dic2 설정을 위해서 pd.date_range에서 날짜 리스트를 뽑는다.
"""
start = '2020-04-01'
end = '2020-04-02'

for date in pd.date_range(start=start, end=end, freq='1D'):
    Dic2 = 'date=' + date.strftime('%Y-%m-%d')
    print(Dic2)
    files = glob.glob(Dic1 + '/' + Dic2 + '/*.parquet')

    """저장할 디렉토리를 생성한다."""
    try:
        if not os.path.exists(Dic1 + '/' + Dic2 + '/description(chunk)/'):
            os.makedirs(Dic1 + '/' + Dic2 + '/description(chunk)/')
    except OSError:
        print('Error: Creating directory.')

    with open('description.json', 'r') as f:
        descript = json.load(f)
    num = 1
    for file in files:
        df_chunk = pq.read_table(file)
        for i in range(5): #이 for문은 5 덩어리로 나누는 것을 의미함.
            # print(int(len(df_chunk) / 4) * i, int(len(df_chunk) / 4) * (i + 1))
            df = df_chunk[int(len(df_chunk) / 4) * i:int(len(df_chunk) / 4) * (i + 1)].to_pandas()
            for key in descript.keys():
                if ('1WAY' in key) or ('DUCT' in key) or ('4WAY' in key) or ('360_CST' in key) or ('CEILING' in key):
                    print("key : {} - time : {}".format(key, datetime.datetime.now()))
                    uuid = descript[key]
                    data = df[df['uuid'].isin(uuid)]
                    data = data.reset_index(drop=True)
                    data['timestamp'] = pd.to_datetime(data['timestamp'])
                    data = data.sort_values('timestamp')
                    data.to_parquet(Dic1 + '/' + Dic2 + '/description(chunk)/' + key + '({}-{}).parquet'.format(num, i),
                                    index=False)
                    del data
            del df
            gc.collect() # 메모리 관리를 위해서 작동함.
        del df_chunk
        gc.collect()
        num +=1