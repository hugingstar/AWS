import pandas as pd
import glob
import os
"""
이 코드는 리스트에 추가된 디바이스 타입, 그 안의 uuid 별로 저장하는 것임.
"""
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 600)

"""가장 처음이 되는 디렉토리 : 이 폴더와 동일한 위치에 코드가 위치해야함.
이 폴더 안에 들어있는 parquet파일을 순서대로 불러와서 함."""
Dic1 = 'devicetype=Air-Conditioner(descript)'

"""저장할 디렉토리"""
Dic2 = 'devicetype=Air-Conditioner(uuid)'

"""마지막에 최종적으로 저장할 때 사용하는 데이터 프레임임."""
data = pd.DataFrame()
total = pd.DataFrame()

"""디바이스 종류명을 리스트에 추가"""
descript = ['1WAY', '4WAY', '360_CST', 'CEILING', 'DUCT', 'FCU_1WAY', 'FCU_4WAY', 'FRESH_DUCT']

for name in descript:
    """저장할 디렉토리 생성 : 디바이스 타입(descript)별로 저장하는 것임."""
    try:
        if not os.path.exists(Dic2 + '/' + name):
            """description 없으면 디렉토리 생성"""
            os.makedirs(Dic2 + '/' + name)
        else:
            pass
    except OSError:
        print('Error: Creating directory.')

    """for문에 들어가서 처리할 대상 파일 설정"""
    files = glob.glob(Dic1 + '/' + name + '/' + '*.parquet')
    for file in files:
        """호출할 데이터 파일 명 확인"""
        """데이터 호출"""
        df = pd.read_parquet(file, engine='pyarrow')
        df['timestamp'] = pd.to_datetime(df['timestamp']) #timestampe를 date time으로 바꾼다.
        for uuid in df.uuid.unique():
            # print("file : {} - uuid : {}".format(file, uuid))
            data = df[df['uuid'] == uuid]
            # print(data.shape)

            """ Devicetype --> uuid 별로 분류 작업을 시작"""
            if not len(data) == 0: #데이터가 있다면,
                data = data.reset_index(drop=True) #인덱스를 리셋하고, 원래 있던 것은 제거
                temp = []
                for i in data.index:
                    """tags 컬럼 처리"""
                    if type(data.loc[i, 'tags']) == str:
                        tag = data.loc[i, 'tags'].replace(' ', ',') #데이터 내부에 ' '을 ','으로 교체
                        # print("type : {} - tag : {}".format(type(tag), tag))
                    else:
                        tag = data.loc[i, 'tags'] #type이 numpy임
                        # print("type : {} - tag : {}".format(type(tag), tag))
                    resource = str(data.loc[i, 'resourceName']) #Temperatures
                    # print(resource)
                    """
                    1) tag 안에있는 값이 str 타입
                    """
                    if type(tag) == str: # tag 가 str 타입인 경우에
                        # print("type : {} - tag : {}".format(type(data.loc[i, resource]), data.loc[i, resource]))

                        """
                        1-1)tag-str-값이 있을 때
                        """
                        if (len(eval(tag))) > 1: # tag 안에 값이 있으면
                            if pd.isnull(data.loc[int(i), resource]) == True: #tag안에 값이, nan값이 있는 경우에는 빈칸으로 남긴다.
                                d = {}
                            elif data.loc[int(i), resource][0] == '[': #처음 값이 '['이면
                                d = data.loc[int(i), resource].replace('{', ',{') #먼저 생긴것을 통일 시킴
                                d = d.replace("[,{", "[{") # 먼저 생긴것을 통일 시킴
                                if int(len(eval(d))) <= 0: #값이 없으면
                                    d = {} #빈칸으로 출력
                                else:
                                    d =eval(d)[0] #리스트의 0번째 값을 출력함. dict 형태로 만들어줌
                            else:
                                if 'array' in data.loc[int(i), resource]:
                                    if pd.isnull(data.loc[int(i), resource]) == True: #nan값이 있는 경우에는 빈칸으로 남긴다.
                                        d ={} #빈칸으로 남김.
                                    else:
                                        """문자열을 처리하는 것임."""
                                        # print(type(data.loc[i, resource]))
                                        d = data.loc[i, resource].replace('array(', '') # 'array(' 문자열를 ''으로 바꿈.
                                        d = d.replace(', dtype=object)', '').replace('dtype=object)', '').replace(' ,', '') #윗줄과 동일
                                        d = eval(d) #dict 형태로 만들어줌

                            """
                            1-2)tag-str-값이 있는데 그게 nan일 때
                            """
                        elif pd.isnull(data.loc[int(i), resource]) == True: #tag에 들어간 값이 nan이면,
                            d = {} #빈칸으로 남긴다.

                            """
                            1-3)tag-str-값이 있는데 그게 array가 있을 때
                            array라는게 행렬이 아니라 그냥 글자임. 그래서 문자열로 처리해주는 것임. 
                            """
                        elif 'array' in data.loc[int(i), resource]: #'array'
                            """1-3-1)tag-str-array값이 있는데 그게 nan일 때"""
                            if pd.isnull(data.loc[int(i), resource]) == True: #nan값
                                d = {} #빈칸으로 남긴다.
                                """1-3-2)tag-str-array값이 있는데 그게 nan아닐 때"""
                            else:
                                d = data.loc[i, resource].replace('array(', '') #'array('라는 문자열을 ''으로 변경
                                d = d.replace(', dtype=object)', '').replace('dtype=object)', '').replace(' ,', '') #윗줄과 동일
                                d = eval(d) #dict 형태로 만들어줌

                            """
                            1-4) 위의 1-1,2,3) 케이스에 속하지 않은 값을 처리
                            """
                        else:
                            if pd.isnull(data.loc[i, resource]) == True: #nan 값이 있으면,
                                d = {} #빈칸으로 출력
                            else: #위의 케이스에 속하지 않았는데 nan 값이 없다면
                                d = eval(data.loc[i, resource]) #dict 형태로 만들어줌

                        """
                        2) tag 안에있는 값이 str 타입이 아니고 다른 타입일 때
                        """
                    else:
                        # print("type : {} - tag : {}".format(type(data.loc[i, resource]), data.loc[i, resource]))
                        if pd.isnull(data.loc[int(i), resource]) == True: #str은 아닌데 nan이 있을 때
                            d ={} #빈칸으로 남긴다.
                        else:
                            if len(data.loc[int(i), resource]) == 0: #0이라는 값이 있다면
                                d = {} #빈칸으로 남겨라
                            else:
                                if type(data.loc[int(i), resource]) == dict: # 0은 아닌데, 그 타입이 dict이면,
                                    d = data.loc[i, resource] #dict 그대로 뽑으면 됨.
                                else:
                                    d = data.loc[int(i), resource][0] #dict가 아니면, [{}] 형태의 list이니까 0번째 값을 출력.
                    """
                    여기까지 수행하면,
                    d = {'current': None, 'desired': 23.0, 'desiredHeat': None, 'id': '0', 'increment': None, 'maximum': None, 'minimum': None, 'unit': None}
                    모양으로 나옴.

                    dict가 만들어졌는데, items()는 dict에서 각각을 tuple 형태로 뽑아냄.
                    resource 라는 열에서 Temperatures 같은 것을 뽑아내는 것임.
                    그니까 지금 여기는 위에 있는 d 에서 하나하나씩 새로운 형태의 dict를 만들어주는 것임.
                    d = {'Temperatures(current)': None, 'Temperatures(desired)': 23.0, 'Temperatures(desiredHeat)': None, 'Temperatures(id)': '0', 'Temperatures(increment)': None, 'Temperatures(maximum)': None, 'Temperatures(minimum)': None, 'Temperatures(unit)': None}
                    모양이 바뀜.
                    """
                    d = {'{}({})'.format(resource, k): v for k, v in d.items()}
                    # print(d)
                    """
                    이 과정은 timestamp가 아주 유니크 하기 때문에 위의 딕셔너리 d 에다가 추가시켜줘서,
                    이 d는 아주 고유한 d라는 것을 선언해주는 것임.
                    이 과정을 거치고 나서 d의 출력은
                    d = {'Temperatures(current)': None, 'Temperatures(desired)': None, 'Temperatures(desiredHeat)': None, 'Temperatures(id)': '0', 'Temperatures(increment)': '1.0', 'Temperatures(maximum)': None, 'Temperatures(minimum)': None, 'Temperatures(unit)': None, 'timestamp': Timestamp('2020-04-02 02:52:10.791000+0000', tz='UTC')}
                    으로 timestamp key와 value가 추가되었음.
                    """
                    d['timestamp'] = data.loc[i, 'timestamp']
                    # print(d)
                    """
                    모든 처리가 완료 되었으므로, 리스트에 추가시킨다.
                    """
                    temp.append(d)
                    del d # 메모리가 너무 커지니까 작업이 끝나면, del을 사용하여 꼭 꺼주길 바랍니다.
                """
                모든 처리가 완료된 temp리스트를 데이터 프레임을 선언하여 데이터프레임으로 만들어 준다.
                """
                temp = pd.DataFrame(temp)

                """Index : timestamp"""
                # data라는 변수에는 하루치를 연산한 것을 순서대로 추가 시켜서 저장하는 것임.
                # 그러니까 이것은 전체 기간이 나온 파일이 나오는 것임.
                data = pd.merge(data, temp, how='outer', on='timestamp')
                del temp #메모리가 너무 커지니까 작업이 끝나면, del을 사용하여 꼭 꺼주길 바랍니다.
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                data = data.sort_values('timestamp')

                """Index : eventTime""" #깔끔하게 저장할려고 한거임.
                total = pd.concat([total, data])
                total.index = pd.to_datetime(total['eventTime'])
                total = total.sort_index()
                total.to_csv(Dic1 + '/' + Dic2 + '/' + name + '/' + '{}_{}.csv'.format(name, uuid))