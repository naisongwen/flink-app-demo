import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.TypeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import sun.reflect.FieldInfo;

import java.io.File;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class TestFlinkSQL {
    Schema schema = new Schema(
            Types.NestedField.optional(1, "WAFER_ID", Types.StringType.get())
    );

    class TestObj implements Serializable {
        public String getWAFER_ID() {
            return WAFER_ID;
        }

        public void setWAFER_ID(String WAFER_ID) {
            this.WAFER_ID = WAFER_ID;
        }

        String WAFER_ID;
    }

    class IcebergSinkOperator extends AbstractStreamOperator<Row>
            implements OneInputStreamOperator<Tuple2<Boolean,Row>,Row> {

        @Override
        public void processElement(StreamRecord<Tuple2<Boolean,Row>> element) throws Exception {
            Row row= element.getValue().f1;
            output.collect(new StreamRecord(row, System.currentTimeMillis()));
        }
    }


    TableSchema tableSchema() {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        return schemaBuilder
                .field("WAFER_ID", DataTypes.STRING()).build();
    }

    private RowTypeInfo getRowTypeInfo() {
        return new RowTypeInfo(new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO,LocalTimeTypeInfo.LOCAL_DATE_TIME} ,new String[] {"Eid","CreateDate"});
    }

    @Test
    public void testFlinkMapping() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        String kafkaServers = "10.201.0.89:9092";
        String topicName="testTopic5";
        String  sql=String.format("CREATE TABLE NcovLimsurvey (\n" +
                " Eid int NULL,\n" +
                " CreateDate timestamp\n" +
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
        org.apache.flink.table.api.Table table=tableEnv.sqlQuery("select * from NcovLimsurvey");
        DataStream<Tuple2<Boolean,Row>> resultStream = tableEnv.toRetractStream(table, getRowTypeInfo());
//        DataStream<Row> resultStream2 = tableEnv.toAppendStream(table, getRowTypeInfo());
        Table tt=tableEnv.fromDataStream(resultStream);
        tt.printSchema();
        tableEnv.createTemporaryView("NcovLimsurvey",tt);
        org.apache.flink.table.api.Table table2=tableEnv.sqlQuery("select Eid,CreateDate from NcovLimsurvey");

        CloseableIterator<Row> closable=table2.execute().collect();
        if(closable.hasNext()) {
            Row row = closable.next();
            System.out.println(row);
        }
        env.execute();
    }

    @Test
    public void testFlinkUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        String kafkaServers = "10.201.0.89:9092";
        String topicName="testTopic3";
        String sql=String.format("create table testTable(WAFER_ID string" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
                "  -- required: specify the Kafka server connection string\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
                "  'properties.group.id' = 'testGroup1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
                "  'format' = 'dsg-json' -- required: Kafka connector requires to specify a format,\n" +
                ")",topicName,kafkaServers);
        tableEnv.executeSql(sql);
        Table table1=tableEnv.sqlQuery("select * from testTable");
        DataStream<Tuple2<Boolean,Row>> stream1 = tableEnv.toRetractStream(table1, Row.class);

        Table table2=tableEnv.sqlQuery("select * from testTable");
        DataStream<Tuple2<Boolean,Row>> stream2 = tableEnv.toRetractStream(table2, Row.class);
        Map<String, String> baseProperties = ImmutableMap
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hadoop")
                .of("warehouse", "target/"
                );
        CatalogLoader catalogLoader = CatalogLoader.hadoop("iceberg_default", new Configuration(), baseProperties);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("default"),"testTable");
        String tabPath="target/default/testTable";
        File path=new File(tabPath);
        if (path.exists()) {
            FileUtils.cleanDirectory(path.getParentFile());
            path.mkdirs();
        }
        catalog.createTable(identifier,schema,null);
        //DataStream<Tuple2<Boolean,Row>> unionSinkStream=stream1;//.union(stream2);//.transform("toRow", TypeInformation.of(Row.class), new IcebergSinkOperator());
        DataStream<Row> unionSinkStream=stream1.transform("toRow", TypeInformation.of(Row.class), new IcebergSinkOperator());
        unionSinkStream.print();

        TableLoader tableLoader = TableLoader.fromHadoopTable("target/default/testTable");
        FlinkSink.forRow(unionSinkStream, tableSchema())
                .tableLoader(tableLoader)
                .tableSchema(tableSchema())
                .build();
        env.execute();
    }


    @Test
    public void testFlinkUnion2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        env.getCheckpointConfig().setCheckpointInterval(3*1000);
        String kafkaServers = "10.201.0.89:9092";
        String topicName="testTopic3";
        String sql=String.format("create table testTable(WAFER_id string" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
                "  -- required: specify the Kafka server connection string\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
                "  'properties.group.id' = 'testGroup2',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
                "  'format' = 'dsg-json' -- required: Kafka connector requires to specify a format,\n" +
                ")",topicName,kafkaServers);
        tableEnv.executeSql(sql);
        Table table1=tableEnv.sqlQuery("select * from testTable");
        DataStream<Tuple2<Boolean,Row>> stream1 = tableEnv.toRetractStream(table1,Row.class);
        stream1.print();

        Table table2=tableEnv.sqlQuery("select * from testTable");
//        DataStream<Tuple2<Boolean,TestObj>> stream2 = tableEnv.toRetractStream(table2, TestObj.class);
        Map<String, String> baseProperties = ImmutableMap
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hadoop")
                .of("warehouse", "target/"
                );
        CatalogLoader catalogLoader = CatalogLoader.hadoop("iceberg_default", new Configuration(), baseProperties);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("default"),"testTable");
        String tabPath="target/default/testTable";
        File path=new File(tabPath);
        if (path.exists()) {
            FileUtils.cleanDirectory(path.getParentFile());
            path.mkdirs();
        }
        org.apache.iceberg.Table table=catalog.createTable(identifier,schema,null);
//        DataStream<Tuple2<Boolean,Row>> unionSinkStream=stream1;//.union(stream2);//.transform("toRow", TypeInformation.of(Row.class), new IcebergSinkOperator());
        DataStream<Row> unionSinkStream=stream1.map(v->v.f1);//transform("toRow", TypeInformation.of(Row.class), new IcebergSinkOperator());
//        unionSinkStream.print();

        TableLoader tableLoader = TableLoader.fromHadoopTable("target/default/testTable");
        FlinkSink.forRow(unionSinkStream, tableSchema())
                .tableLoader(tableLoader)
                .tableSchema(tableSchema())
                .build();
        env.execute();
    }
}
