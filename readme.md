### 프로젝트 폴더 구성입니다.

- ./pyspark/medi05-pyspark-preprocessor.ipynb
    - pyspark 전처리 코드입니다. 
    - 1번의 준비와, 7번의 upload to redshift에서 경로/변수 할당이 필요합니다.

- ./pyspark/merge-manager.py
    - csv 결과 파일들을 병합하는 코드입니다.

- ./pyspark/queue-manager.py
    - csv 큐 작업 목록을 생성하는 코드입니다.

- ./pyspark/run.sh
    - 실행 전 chmod +x ./pyspark/run.sh
    - 전체 코드를 실행하는 쉘 스크립트 코드입니다.

- ./spark/medi05-scala-preprocessor.ipynb
    - scala 전처리 코드입니다. 
    - 1번의 준비와, 7번의 upload to redshift에서 경로/변수 할당이 필요합니다.

- ./utils/json-splitter.ipynb
    - json 파일을 list slicing하고, dictionary로 만드는 코드입니다.
    - 로컬 기준으로 작성된 코드입니다.

- ./utils/json-preprocessor.ipynb
    - json 파일을 dictionary로 만드는 코드입니다.
    - emr 환경을 기준으로 작성된 코드입니다.