docker cp -L ../target/spark-batch-example-1.0-SNAPSHOT-jar-with-dependencies.jar spark-spark-master-1:/opt/bitnami/spark/spark-batch-example-1.0-SNAPSHOT.jar

docker-compose exec spark-master spark-submit --class org.example.Main --master spark://spark-master:7077 spark-batch-example-1.0-SNAPSHOT.jar
# chmod +x init.sh