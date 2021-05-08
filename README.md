# AWS S3에 업로드/다운로드하기
   * 아래에 적힌 글은 터미널을 사용하여 다운로드 하는 방법인데, 시간없으면 터미널이 편함.
   * 패키지 사용해서 하는 방법은 코드를 첨부함.
   * 필요한 패키지 : boto3, botocore

### 1.AWS(Amazon Web Service)

   * S3 :  Simple Storage Service로 클라우드 저장소임. 지역(Region)별로 있는 저장소에 접속하는 개념임.
   * (서울 : ap-northeast-2 처럼 버킷이 등록된 고유 Region이 있음)
   * Bucket : 쉽게 생각하면, S3 상에서 저장하고 싶은 디렉토리를 말함.
   * 내가 버킷을 만들면, 내가 만들 버킷에 접속하 수 있는 권한을 부여한 Accesskey, Secretkey를 생성할 수 있음.
   * Accesskey, Secretkey가 있다고해서 모든 버킷 내부 디렉토리에 들어갈 수 있는 것이 아니라, 추가적인 접근 허가를 해주어야함.


### 2.Bucket 만들기

   * 버킷을 만드는 방법은 간단함. S3 가입하고, Bucket 생성 진행하면 손쉽게 만들 수 있음.
   * IAM 에서는 버킷에 접속할 수 있는 Accesskey, Secretkey를 만들 수 있음.
   * 접속 키는 반드시 인식 가능한 사람에게만 공유, 잊어버리면 찾는 방법이 없음.


### 3.AWS S3에서 파일 다운로드

   * 라이브러리 다운로드가 안되어 있다면 여기서부터 시작
   * 대중적으로 많이 사용하는 데이터 저자 파일 확장자(Extenstion)는 "*.csv", "*.parquet" 이 사용됨.
   * 관리자는 확장자에 따라서 접속한 사용자에게 다운로드 권한을 부여할 수 있음.
   * 파일을 다운로드 받느 방법은 다양하게 있지만, 터미널에서 awscli를 사용하 방법이 가장 단순하면서도 빠르게 다운로드 할 수 있음.
   * pip3 사용한 이유는 한글이 깨지는 문제가 있어서...(크게 중요한 것은 아님.)


#### boto3 다운로드

   * boto3은 파이썬 코드를 사용해서 다운로드/업로드 하기 위해서 편집창에서 사용함.
   
    pip3 install boto3


#### awscli 다운로드

   * awscli를 다운로드 하고 터미널에서 접속
   
    pip3 install awscli


#### aws 접속 및 정보등록

   * aws configure을 사용하여 접속 키 입력하여 S3에 접속
   

    # 접속 및 정보등록
    aws configure
    
    AWS Access Key ID [None]: "액세스 키 입력"
    AWS Secret Access Key [None]: "시크릿 키 입력"
    Default region name [None]: ap-northeast-2
    Default output format [None]: json

    # Accesskey, Secretkey 확인
    aws configure list


#### aws s3 디렉토리 확인

   * 파일 확장자명을 살펴보면, "*.parquet", "*.csv" 등 다양한 파일들이 업로드되어 있음.
   
    # 파일 디렉토리 확인
    aws s3 ls

    # 세부 오브젝트 확인
    aws s3 ls "Bucket Name"


#### S3 저장소에서 파일 다운로드/업로드하기

   * 터미널에서 간단하게 다운로드하는 방법
   * 로컬 디렉토리에 폴더가 생성되어 있지 않아도 다운로드 하면서 생성됨.
   * 폴더까지만 쓰면 하위 파일이 모두 다운로드됨.
   * sync, cp를 사용하여 다운로드 가능.
   * 접속 거부인 상태에서는 403 에러 발생하는 것을 유의하기.

    # Download
    aws s3 sync S3://BucketName/ D://Local/ --request-payer requester

    # Upload
    aws s3 sync D://Local/ S3://BucketName/ --request-payer requester


#### Amazon S3 ID 확인 방법
   * 간단히 확인 가능, 기본적으로 s3 객체느 해당 객체를 업로드한 AWS 계정의 소유임.
   * ID가 일치하지 않는 경우에는 사용자(버킷소유자)가 객체를 소유하고 있지 않는 것임.
  
    #버킷리스트 확인
    aws s3api list-buckets

    #Owner id 확인
    aws s3api list-buckets --query Owner.ID

    #세부항목들의 Owner id 확인 방법
    aws s3api list-objects --bucket BucketName --prefix prefix
  
    #계정 조회
    aws sts get-caller-identity


  [Husingstar's Blog](https://hugingstar.github.io/awss3/)