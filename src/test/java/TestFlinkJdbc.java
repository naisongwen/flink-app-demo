import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class TestFlinkJdbc {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        String kafkaServers = "10.201.0.89:9092";
        String topicName="testTopic5";
        String  sql=String.format("CREATE TABLE kafkaTable (\n" +
                " id int,\n" +
                " CreateDate timestamp,\n" +
                " proctime AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
                "  -- required: specify the Kafka server connection string\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
                "  'properties.group.id' = 'testGroup-3',\n" +
                "  'properties.max.poll.records' = '200',\n" +
                "  'scan.startup.mode' = 'timestamp',\n" +
                "  'scan.startup.timestamp-millis' = '%d',\n" +
                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
                "  'format' = 'dlink-json' -- required: Kafka connector requires to specify a format,\n" +
                ")",topicName,kafkaServers,0);
        tableEnv.executeSql(sql);

        sql = String.format("create table postgrelTable(" +
                "id int," +
                "NAME string" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '%s',\n" +
                "  'table-name' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '%s'\n" +
                ")", "jdbc:postgresql://10.201.0.212:5432/postgres", "testTable", "postgres", "postgres");
        tableEnv.executeSql(sql);
        String query = String.format(
                "select kafkaTable.* from kafkaTable JOIN postgrelTable" +
                        " FOR SYSTEM_TIME AS OF proctime AS postgresql " +
                        "on  kafkaTable.id=postgresql.id");

        Table table = tableEnv.sqlQuery(query);
        CloseableIterator<Row> closable = table.execute().collect();
        if (closable.hasNext())
            System.out.println(closable.next());
    }
}
