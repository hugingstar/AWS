import pandas as pd
import glob
import os
import json
import multiprocessing as mp
from multiprocessing import Pool
import numpy as np

"""April"""
for i in range(20, 31):
    if i < 10:
        Dic2 = 'date=2020-04-0{}'.format(i)
    elif i >= 10:
        Dic2 = 'date=2020-04-{}'.format(i)
    files = glob.glob(Dic2 + '/*.parquet')

    try:
        if not os.path.exists(Dic2 + '/description'):
            os.makedirs(Dic2 + '/description')
    except OSError:
        print ('Error: Creating directory.')

    with open('description.json', 'r') as f:
        descript = json.load(f)

    num = 1
    for file in files:
        print(Dic2, num)
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
                data.to_parquet(Dic2 + '/description/' + key + '({}).csv'.format(num), index=False)
        num += 1
