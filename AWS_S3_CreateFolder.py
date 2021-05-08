"""해당 디렉토리에 폴더 새로 만들기"""
import os

def CreartFolder(directory): #디렉토리를 인자로 받고
    try:
        if not os.path.exists(directory): #디렉토리 경로에 없으면
            os.makedirs(directory) #만들고
    except OSError: #OSError 처리
        print('Error:Creating directory.' + directory) #메시지 출력
    print("[Message] Create directory!!!") #메시지 출력

mk_file ='ExampleFolder'
m1_path = 'D:/{}'.format(mk_file) #경로는 사용자 입맛에 맞게 지정
CreartFolder(m1_path)