### set paths
FILE_PATH="./queue.txt"
QUEUEMANAGER_PATH="./queue-manager.py"
PREPROCESSOR_PATH="./medi05-pyspark-preprocessor.py"
MERGEMANAGER_PATH="./merge-manager.py"

### create queue text file
python $QUEUEMANAGER_PATH

### unset envs (local jupyter lab 환경 변수와 충돌 발생하기 때문에 unset)
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

### spark-submit
count=0
# queue text file이 있다면 while문이 실행
while [ -f $FILE_PATH ]
do
    # 공백을 제외하고 다른 값이 없으면 queue가 없다고 판단함
    if ! cat $FILE_PATH | grep -q '[^[:space:]]'; then
        echo "a queue text file is empty."
        break
    fi
    count=$((count+1))
    echo "Job: $count"
    spark-submit \
        --conf "spark.driver.bindAddress=127.0.0.1" \
        --master local[*] \
        $PREPROCESSOR_PATH
done

### merge csvs
spark-submit \
        --conf "spark.driver.bindAddress=127.0.0.1" \
        --master local[*] \
        $MERGEMANAGER_PATH