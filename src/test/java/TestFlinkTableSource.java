import com.dlink.health.common.SysEmployee;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.jetty.util.ajax.JSON;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

//JdbcLookupTableITCase
public class TestFlinkTableSource {
    //jdbc:postgresql://10.201.0.212:5432/postgres postgres postgres testTable
    public static void main(String[] args) throws Exception {
        String url = args[0];
        String user = args[1];
        String passwd = args[2];
        String table = args[3];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        String kafkaServer = "10.201.0.89:9092";
        String topicName = "testTopic4";
        String groupId = "test-group";
        KafkaSource<PartitionAndValue> kafkaSource =
                KafkaSource.<PartitionAndValue>builder()
                        .setClientIdPrefix("KafkaSourceReaderTest")
                        .setDeserializer(new TestingKafkaRecordDeserializer())
                        .setPartitions(Collections.singleton(new TopicPartition(topicName, 0)))
                        .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
                        .setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                        .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        // Start from committed offset, use EARLIEST as reset strategy if committed offset doesn't exist
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build();

        SingleOutputStreamOperator<SysEmployee> kafkaStream = env.
                fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka").
                map(new MapFunction<PartitionAndValue, SysEmployee>() {

                    @Override
                    public SysEmployee map(PartitionAndValue partitionAndValue) {
                        Map<String, Object> map = (Map<String, Object>) new JSON().fromJSON(partitionAndValue.value);
                        SysEmployee sysEmployee = new SysEmployee();
                        sysEmployee.setEid((Integer) map.get("Eid"));
                        sysEmployee.setBadge((String) map.get("Bage"));
                        return sysEmployee;
                    }
                });

        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDBUrl(url)
                .setTableName(table)
                .setUsername(user)
                .setPassword(passwd)
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING()).build();

        JdbcTableSource jdbcTableSource = JdbcTableSource.builder()
                .setOptions(jdbcOptions)
                .setSchema(tableSchema)
                .build();

        tableEnv.createTemporaryView("kafka", kafkaStream,$("Eid"),$("Badge"));

        String kafkaServers = "10.201.0.89:9092";
        String  sql=String.format("CREATE TABLE NcovLimsurvey (" +
                "Eid int,CreateDate timestamp\n" +
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

        //createTemporarySystemFunction does not work
        tableEnv.registerFunction("jdbcLookup", jdbcTableSource.getLookupFunction(new String[]{"id"}));
        String sqlQuery = "select Eid from NcovLimsurvey,lateral table(jdbcLookup(Eid)) AS T(id, name)";
        Table resultTable = tableEnv.sqlQuery(sqlQuery);
        tableEnv.executeSql(sqlQuery).collect();
    }

    private static class PartitionAndValue implements Serializable {
        private static final long serialVersionUID = 4813439951036021779L;
        private final String tp;
        private final String value;

        private PartitionAndValue(TopicPartition tp, String value) {
            this.tp = tp.toString();
            this.value = value;
        }
    }

    private static class TestingKafkaRecordDeserializer
            implements KafkaRecordDeserializer<PartitionAndValue> {
        private static final long serialVersionUID = -3765473065594331694L;
        private transient Deserializer<String> deserializer;

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record, Collector<PartitionAndValue> collector)
                throws Exception {
            if (deserializer == null) {
                deserializer = new StringDeserializer();
            }
            collector.collect(
                    new PartitionAndValue(
                            new TopicPartition(record.topic(), record.partition()),
                            deserializer.deserialize(record.topic(), record.value())));
        }

        @Override
        public TypeInformation<PartitionAndValue> getProducedType() {
            return TypeInformation.of(PartitionAndValue.class);
        }
    }
}
