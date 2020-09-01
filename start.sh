
cd /root/baseBotStream;
mkdir /tmp/spark-events;
sh /usr/local/spark/sbin/start-all.sh
#hdfs dfs -rm -r /base-bot-stream-0;
hdfs dfs -touch /sparkShutdownMarkers-base-bot-stream-0;
sbt assembly;
start-dfs.sh;
hdfs dfsadmin -safemode leave;
spark-submit --driver-memory "15g" --driver-java-options "-Duser.timezone=IST -XX:+UseCompressedOops -XX:+UseG1GC -javaagent:/usr/local/spark/jars/jmx_prometheus_javaagent-0.13.0.jar=81:/usr/local/spark/conf/spark.yml" --class  ai.jubi.ProcessStream /root/baseBotStream/target/scala-2.12/basebotstream-assembly-1.0.jar;


