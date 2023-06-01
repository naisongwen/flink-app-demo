package com.dlink.health;

import com.dlink.health.common.AlertEmploySchema;
import com.dlink.health.common.CatalogUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class HealthCodeFlinkApp3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.getCheckpointConfig().setCheckpointInterval(5 * 60 * 1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaServers = parameterTool.get("kafkaServers", "10.201.0.89:9092");
        String V_NcovLimsurveyTopic = parameterTool.get("V_NcovLimsurvey");
        String dbServerUrl = parameterTool.get("dbServerUrl", "jdbc:sqlserver://10.201.0.212:1433;DatabaseName=testDB;");
        String dbServerUserName = parameterTool.get("dbServerUserName", "sa");
        String dbServerPasswd = parameterTool.get("dbServerPasswd", "Wm@12345");
        Integer dbServerReloadInterval=parameterTool.getInt("dbServerReloadInterval",12);
        String thriftUri = parameterTool.get("thriftUri", "10.201.0.212:9083");
        String warehouse = parameterTool.get("warehouse", "/user/hive/warehouse/");
        String corpid = parameterTool.get("corpid");
        String corpsecret = parameterTool.get("corpsecret");
        String toUsers = parameterTool.get("toUsers");
        String tenantId = parameterTool.get("tenantId");
        int projId = parameterTool.getInt("projId");
        String catalogName = parameterTool.get("catalogName", "iceberg-default");
        String databaseName = parameterTool.get("databaseName", "default");
        String icebergTableName = parameterTool.get("icebergTableName", "alertEmployee");
        String CreateDate = parameterTool.get("CreateDate", "yyyy-MM-dd HH:mm:ss");
        Long kafkaStartMills = parameterTool.getLong("kafkaStartMills");

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        CreateDate=CreateDate.replace("T"," ");
        LocalDateTime startDate = LocalDateTime.parse(CreateDate, dtf);

        String sql = String.format("CREATE TABLE V_NcovLimsurvey (\n" +
                " CreateDate timestamp,\n" +
                " IsValid int,\n" +
                " Q1 string,\n" +
                " Q3 string,\n" +
                " Q74 string\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'dlink-json.ignore-parse-errors' = 'true',\n" +
                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.group.id' = 'testGroup-4',\n" +
                "  'scan.startup.mode' = 'timestamp',\n" +
                "  'scan.startup.timestamp-millis' = '%d',\n" +
                "  'format' = 'dlink-json' -- required: Kafka connector requires to specify a format,\n" +
                ")", V_NcovLimsurveyTopic, kafkaServers, kafkaStartMills);
        tableEnv.executeSql(sql);

        org.apache.flink.table.api.Table NcovLimsurveyResult=tableEnv.sqlQuery(String.format("select * from V_NcovLimsurvey where CreateDate>='%s'",CreateDate));
        DataStream<Tuple2<Boolean,Row>> NcovLimsurveyResultStream=tableEnv.toRetractStream(NcovLimsurveyResult, Row.class);
        FilterFunction<Row> ff = new FilterFunction<Row>(){

            @Override
            public boolean filter(Row r) {
                return (r.getField(4) !=null && r.getField(4).toString().startsWith("是")) && (r.getField(1) !=null && ((int)r.getField(1)) == 1);
            }
        };
        HealthMapFunction NcovLimsurveyFlatMapFunction=new HealthMapFunction("厂商防疫问卷-实时",corpid,corpsecret,toUsers,dbServerUrl,dbServerUserName,dbServerPasswd,dbServerReloadInterval);

        SingleOutputStreamOperator<Row> NcovLimsurveyResultFlatMapStream =NcovLimsurveyResultStream.map(v->v.f1).startNewChain().
                filter(ff).
                flatMap(NcovLimsurveyFlatMapFunction);

        TableSchema icebergTableSchema=AlertEmploySchema.NcovLimsurveyTableSchema();
        CatalogUtil.reCreateIcebergTableIfExist(tenantId, projId, catalogName, databaseName, icebergTableName, icebergTableSchema, Lists.newArrayList(), new HashMap<>(), "table for parsed row data", false, true);
        String catalogMappingName = CatalogUtil.icebergCatalog.getCatalogMappingName();
        TableIdentifier icebergTableIdentifier = TableIdentifier.of(databaseName, icebergTableName);
        CatalogLoader catalogLoader = CatalogUtil.getHiveCatalogLoader(catalogMappingName, warehouse,thriftUri);
        TableLoader AkmResultTableLoader = TableLoader.fromCatalog(catalogLoader, icebergTableIdentifier);
        FlinkSink.forRow(NcovLimsurveyResultFlatMapStream, icebergTableSchema)
                .tableLoader(AkmResultTableLoader)
                .tableSchema(icebergTableSchema)
                .build();
        env.execute(HealthCodeFlinkApp3.class.getName());
    }
}
