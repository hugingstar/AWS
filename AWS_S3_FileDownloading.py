"""AWS S3에서 파일 다운로드하기"""
import boto3
import botocore

bucket_name='hugingstark' # 버킷의 이름

#S3에 등록되어 있는 파일
s3_path = '' #s3 디렉토리 s3://bucket/object... 형식으로 입력하면됨.
in_file = 'input_data_each_pearson2.csv'
#.parquet 등 Anyway 단, 접속 허가받은 상태여야 다운로드가 진행됨.

#내보낼 경로
local_path ='D:/Serving(Pearson)/AWS/Results/' #로컬 경로
out_file = in_file # 내보낼 파일명

s3 = boto3.resource('s3')
try:
    s3.Bucket(bucket_name).download_file(s3_path + in_file, local_path+out_file)
    print('Download complete!!! : {}'.format(bucket_name))
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] =='404': # 이 부분도 접속 허가가 없으면 이런 에러가 뜸
        print('There is not corresponding file in the bucket :{}'.format(bucket_name))
    else:
        raise