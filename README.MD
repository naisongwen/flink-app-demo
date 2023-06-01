#### avro-tools
http://archive.apache.org/dist/avro/avro-1.9.2/java/avro-tools-1.9.2.jar

brew install parquet-tools

#### usage

- java -jar avro-tools-1.9.2.jar tojson *.avro
- parquet-tools meta
- parquet-tools cat -j 

mvn clean  package -Dmaven.test.skip=true
java -cp target/health-code-app-1.1-SNAPSHOT-jar-with-dependencies.jar com.dlink.health.SqlServerConnect