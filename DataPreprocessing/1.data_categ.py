import pandas as pd
import glob
import os
import json
import multiprocessing as mp
from multiprocessing import Pool
import numpy as np

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""이 파일은 devicetype=Air-Conditioner에서 날짜순으로 되어 있는 파일에 들어가서
description에 따라서 분류하는 파이썬 코드입니다.
데이터 : *.parquet 확장자로 되어 있어서 압축이 되어 있기 때문에 
*.csv보다 용량 관리 측면에서 더욱 효율적입니다. 하지만, 사람이 읽을 수 없으므로
최종 상태에서는 확인할 부분에 대해서는 .csv로 바꿀 필요가 있습니다. 
또한, 향후에 *.parquet 파일이 있으면 안에 들어있는 데이터의 타입을 더욱 코드상으로 간편하게 관리할 수 있습니다."""

"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함."""
Dic1 = 'devicetype=Air-Conditioner'

"""분석하고자하는 월 선택 : 
 (ex) 4월 1일부터 30일까지(31설정시)"""
month = '04'
from_day = 13
to_day = 31 #30까지 돌릴려면 31로 하세요.

for i in range(from_day, to_day):
    if i < 10:
        Dic2 = 'date=2020-{}-0{}'.format(month, i)
    elif i >= 10:
        Dic2 = 'date=2020-{}-{}'.format(month, i)

    files = glob.glob(Dic1 + '/' + Dic2 + '/*.parquet')
    print(Dic2)
    try:
        if not os.path.exists(Dic1 + '/' + Dic2 + '/description'):
            """description 없으면 디렉토리 생성"""
            os.makedirs(Dic1 + '/' + Dic2 + '/description')
        else:
            pass
    except OSError:
        print ('Error: Creating directory.')

    with open('description.json', 'r') as f:
        """동일한 디바이스 내에 uuid를 정리해 놓은 파일"""
        descript = json.load(f)

    num = 1
    for file in files:
        print("[{}] {}".format(num, file))
        df = pd.read_parquet(file, engine='pyarrow')
        for key in descript.keys():
            if ('1WAY' in key) or ('DUCT' in key) or ('4WAY' in key) or ('360_CST' in key) or ('CEILING' in key):
                print(key)
                uuid = descript[key]
                data = pd.DataFrame()
                # for id in uuid:
                data = pd.concat([data, df.loc[df['uuid'].isin(uuid)]])
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                data = data.sort_values('timestamp')
                data.to_parquet(Dic1 + '/' + Dic2 + '/description/' + key + '({}).parquet'.format(num), index=False)
        del df
        del data
        num += 1