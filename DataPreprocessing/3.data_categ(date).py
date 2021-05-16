import pandas as pd
import glob
import datetime

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""Devicetype 별로 카테고리화하여 저장하기 때문에 때문에 관심이 있는 타입을 리스트 내에 추가해준다.
최종 결과물은 devicetype=Air-Conditioner(descript) 폴더에 저장합니다."""

descript = ['1WAY', '4WAY', '360_CST', 'CEILING', 'DUCT', 'FCU_1WAY', 'FCU_4WAY', 'FRESH_DUCT']

"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함."""
Dic1 = 'devicetype=Air-Conditioner'


"""분석하려는 기간을 선택"""
start_day = '2020-04-01'
end_day = '2020-04-30'

"""noy_date에는 2.parquet_numbering.py에서 출력된 .txt에 다운로드가 덜된 날짜를 넣으면됨.
10개 중에서 하나라도 모자라면 리스트에 입력하기."""
not_date = ['2020-04-01', '2020-04-09', '2020-04-11', '2020-04-15', '2020-04-23', '2020-04-29']
# not_date = ['2020-05-02', '2020-05-03', '2020-05-05', '2020-05-09', '2020-05-13', '2020-05-21',
#             '2020-05-25', '2020-05-28', '2020-05-31']


for date in pd.date_range(start=start_day, end=end_day, freq='1D'):
    print(date)
    if not date.strftime('%Y-%m-%d') in not_date:
        Dic2 = 'date=' + date.strftime('%Y-%m-%d')
        print(Dic2)

        if date < datetime.datetime(2020, 5, 1): #4월을 돌릴려면 5월 1일을 설정, 5월을 돌릴려면 6월 1일을 설정
            """description 폴더는 1번에서 저장되어 있음."""
            files = glob.glob(Dic1 + '/' + Dic2 + '/description/*.csv')
            if len(files) <= 0: #먼저 csv파일로 확인 ()
                files = glob.glob(Dic1 + '/' + Dic2 + '/description/*.parquet')
        else:
            files = glob.glob(Dic1 + '/' + Dic2 + '/description/*.parquet')

        for name in descript: #decript를 하나씩 입력
            print("Descript name : {}".format(name))
            if (name not in 'FCU') and (name in '1WAY') or (name in '4WAY'): # FCU가 없고 1WAY, 4WAY가 있으면
                filess = [i for i in files if ('FCU' not in i) and (name in i)]
            elif (name in 'FCU') and (name in '1WAY') or (name in '4WAY'): #FCU가 있고 1WAY, 4WAY가 있으면
                filess = [i for i in files if ('FCU' in i) and (name in i)]
            elif (name not in 'FRESH') and (name in 'DUCT'): #FRESH가 없고 DUCT가 있으면
                filess = [i for i in files if ('FRESH' not in i) and ('DUCT' in i)]
            elif (name in 'FRESH') and (name in 'DUCT'): #FRESH가 있고 DUCT가 있으면
                filess = [i for i in files if ('FRESH' in i) and ('DUCT' in i)]
                #추가 조건이 필요하면 아래에 elif로 추가하기
            else:
                filess = [i for i in files if (name in i)] #나머지

            data = pd.DataFrame()
            for file in filess:
                print(file)
                if date < datetime.datetime(2020, 5, 1): #4월을 돌릴려면 5월 1일을 설정, 5월을 돌릴려면 6월 1일을 설정
                    if date.strftime('%Y-%m-%d') == '2020-01-09':
                        df = pd.read_parquet(file, engine='pyarrow')
                    else:
                        df = pd.read_parquet(file, engine='pyarrow')
                else:
                    df = pd.read_parquet(file, engine='pyarrow')
                data = pd.concat([data, df])
            print(data)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            data = data.sort_values('timestamp')
            data = data.dropna(how='all', axis=1)

            """저장 위치 : devicetype=Air-Conditioner(descript)/{name} """
            """parquet으로 저장하고 싶을 때"""
            data.to_parquet(Dic1 + '/' + '(descript)' + '/' + name + '/{}_{}.parquet'.format(name, date.strftime('%Y-%m-%d')), index=False)
            """csv로 저장하고 싶을 때"""
            # data.to_csv(Dic1 + '(descript)/' + name + '/{}_{}.csv'.format(name, date.strftime('%Y-%m-%d')),index=False)
            del data
            del df