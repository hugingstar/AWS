import pandas as pd
import glob
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함.
이 코드는 parquet 확장자 데이터가 모두 다운로드 되었는지 개수를 확인하기 위한 코드입니다.
출력결과는 .txt 파일에 적혀 나옵니다."""

Dic1 = 'devicetype=Air-Conditioner'

"""분석하고자하는 월 선택 : 
 (ex) 4월 1일부터 30일까지(31설정시)"""

month = '04'

f = open("filenumber_{}.txt".format(month), 'w')

for i in range(1, 31):
    if i < 10:
        Dic2 = 'date=2020-{}-0{}'.format(month, i)
    elif i >= 10:
        Dic2 = 'date=2020-{}-{}'.format(month, i)
    files = glob.glob(Dic1 + '/' + Dic2 + '/*.parquet')
    text = "[{}] : {}/{}\n".format(len(files), Dic1, Dic2)
    f.write(text)
    print(text)
f.close()