"""AWS S3에 파일 업로드하기"""
import boto3
bucket_name='hugingstark' # 버킷의 이름
in_file = 'textfile.txt' # 업로드하고 싶은 파일 이름
out_file = in_file # S3에 올려졌을 때 파일명

s3 = boto3.client('s3')

#from 'local_path' to 's3_path'
local_path = 'D:/Serving(Pearson)/AWS/{}'.format(in_file)
s3_path = 'Data/{}'.format(out_file)

s3.upload_file(local_path, bucket_name, s3_path)