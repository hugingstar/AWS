import pandas as pd
import glob
"""data_categ(data)를 돌리고 나서 사용하는 파일 : 디바이스 종류별로 나눈 다음에 데이터를 모두 합침.
출력 결과 :  각 디바이스 종류별 모든 uuid를 합친 파일이 입력됨."""
# descript = ['1WAY', '4WAY', '360_CST', 'CEILING', 'DUCT', 'FCU_1WAY', 'FCU_4WAY', 'FRESH_DUCT']
descript = ['DUCT']
"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함."""
Dic1 = 'devicetype=Air-Conditioner(descript)'

for name in descript:
    """여기에서 호출함. 파일은 하루치씩 연산처리를 함."""
    files = glob.glob(Dic1 + '/' + name + '/' +'*.parquet')

    data = pd.DataFrame()
    for file in files:
        """이 for문을 예를 들어 설명하자면, 1WAY 폴더에 있는 1WAY_2020-04-02.parquet을 하나씩 열어서
        데이터를 모두  concat 하는 것임 물론 컬럼이 동일하니까 아래에 이어 붙이는 것임."""
        print("name : {} - file : {}".format(name, file))

        df = pd.read_parquet(file, engine='pyarrow')

        data = pd.concat([data, df], axis=0) #아래에 이어 붙이기

        """sort value 를 추가 : 이 파일을 확인할 필요가 있으면 활성화하면 됩니다."""
        # data = data.sort_values(by=['uuid', 'timestamp'])
        """데이터가 하루하루 쌓이는 것이 확인하고 싶다면 활성화"""
        # data.to_csv(Dic1 + name + '.csv')
        del df #데이터가 크다보니 메모리 관리를 위해서 df를 한번씩 꺼주는 작업이 필요함.

    data.index = pd.to_datetime(data['eventTime']) #인덱스 설정
    del data['eventTime'] #원래 있던 것은 제거
    data = data.sort_index() #인덱스 시간에 따른 정렬
    """parquet file로 저장하기"""
    # data.to_parquet(Dic1 + '/' + name + '.parquet')
    """csv file로 저장하기"""
    data.to_csv(Dic1 + '/' + name + '.csv')  # 저장
    del data