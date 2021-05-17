"""AWS S3에서 파일 다운로드하기"""
import boto3
import botocore
import os
import pandas as pd
"""
이 코드는 AWS S3 버킷에 접속하여, 버킷(s3://) 내부의 디렉토리의 정보를 가져와
대량의 데이터를 다운로드 하는 코드입니다.

"""
"""버킷의 이름"""
bucket_name ='seoul-ac-data' # 버킷의 이름
bucket_path = 's3://{}/'.format(bucket_name)
"""버킷의 오브젝트: 해당되는 디렉토리를 하나 넣으면, 탐색 범위가 줄어들기 때문에 해놓은 것"""
object_key ='devicetype=Air_Conditioner'

"""boto3.client를 사용하여 AWS 객체 생성"""
s3 = boto3.client('s3')

"""
원하는 다운로드 기간 설정: 1일 단위의 데이터 레인지가 나올 것이므로
"""
start = '2020-08-01'
end = '2021-04-15'

date_list = pd.date_range(start=start, end=end, freq='1D')
date = []
for da in date_list:
    Dic2 = 'date=' + da.strftime('%Y-%m-%d')
    date.append(Dic2)
print(date)
"""
저장하고자 하는 Local 디렉토리를 적어주세요.
devicetype=Air_Conditioner 이전까지의 로컬 경로를 넣어주면 됩니다. 
"""
Dic1 = 'E://datata/'

"""저장할 디렉토리를 생성 기간만큼 나옴."""
try:
    for d in date:
        print(d)
        if not os.path.exists(Dic1 + '/'+ object_key + '/' + d):
            os.makedirs(Dic1 + '/' + object_key + '/' + d)
except OSError:
        print('Error: Creating directory.')
#
for kk in date: # 날짜가 하나씩 들어감.
    try:
        # AWS 경로를 생성 : for 문 어렵게 하지말고 Prefix를 잘 이용하면 간결한 코딩이 가능함.
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='{}/{}'.format(object_key, kk))
        sel_resource = list(response.get('Contents'))
        for name in sel_resource:
            # s3_path = bucket_path + str(name['Key'])
            s3_path = str(name['Key'])
            print(s3_path)
            save_path = Dic1 + str(name['Key'])
            print(save_path)
            """파라미터 : 버킷 이름, s3_path(s://는 빼고 적기), save_path(로컬경로)"""
            s3.download_file(bucket_name, s3_path, save_path)
            # 용량이 크기 때문에 하나씩 다운 받는데 시간이 상당히 걸립니다.

            print("[Download completed] {}".format(save_path))

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] =='404': # 이 부분도 접속 허가가 없으면 이런 에러가 뜸
            print('There is not corresponding file in the bucket :{}'.format(bucket_name))
        else:
            raise