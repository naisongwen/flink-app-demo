package com.dlink.health;

import com.dlink.health.common.AlertEmploySchema;
import com.dlink.health.common.CatalogUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

import java.util.HashMap;

public class HealthCodeFlinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.getCheckpointConfig().setCheckpointInterval(5*60*1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaServers=parameterTool.get("kafkaServers","10.201.0.89:9092");
//        String AkmResultTopic=parameterTool.get("AkmResultTopic");
//        String DaySurveyTopic=parameterTool.get("DaySurveyTopic");
        String NcovLimsurveyTopic=parameterTool.get("NcovLimsurvey");
        String dbServerUrl=parameterTool.get("dbServerUrl","jdbc:sqlserver://10.201.0.212:1433;DatabaseName=testDB;");
        String dbServerUserName=parameterTool.get("dbServerUserName","sa");
        String dbServerPasswd=parameterTool.get("dbServerPasswd","Wm@12345");
        Integer dbServerReloadInterval=parameterTool.getInt("dbServerReloadInterval",12);
        String thriftUri=parameterTool.get("thriftUri","10.201.0.212:9083");
        String warehouse=parameterTool.get("warehouse","/user/hive/warehouse/");
        String corpid=parameterTool.get("corpid");
        String corpsecret=parameterTool.get("corpsecret");
        String toUsers=parameterTool.get("toUsers");
        String tenantId=parameterTool.get("tenantId");
        int projId=parameterTool.getInt("projId");
        String catalogName=parameterTool.get("catalogName","iceberg-default");
        String databaseName=parameterTool.get("databaseName","default");
        String icebergTableName=parameterTool.get("icebergTableName","alertEmployee");
        String CreateDate=parameterTool.get("CreateDate","2022-04-25 17:00:00");
        Long kafkaStartMills=parameterTool.getLong("kafkaStartMills");

//        String sql=String.format("CREATE TABLE AkmResult (\n" +
//                " Eid int NULL,\n" +
//                " HealthState string NULL,\n" +
//                " CreateDate timestamp\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'dlink-json.ignore-parse-errors' = 'true',\n" +
//                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
//                "  -- required: specify the Kafka server connection string\n" +
//                "  'properties.bootstrap.servers' = '%s',\n" +
//                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
//                "  'properties.group.id' = 'testGroup-1',\n" +
//                "  'scan.startup.mode' = 'timestamp',\n" +
//                "  'scan.startup.timestamp-millis' = '%d',\n" +
//                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
//                "  'format' = 'dlink-json' -- required: Kafka connector requires to specify a format,\n" +
//        ")",AkmResultTopic,kafkaServers,kafkaStartMills);
//        tableEnv.executeSql(sql);
//        sql=String.format("CREATE TABLE NCOV_DaySurveyResult (\n" +
//                " Eid int NULL,\n" +
//                " TotalEmpHealthStatus string NULL,"+
//                " CreateDate timestamp\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'dlink-json.ignore-parse-errors' = 'true',\n" +
//                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
//                "  -- required: specify the Kafka server connection string\n" +
//                "  'properties.bootstrap.servers' = '%s',\n" +
//                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
//                "  'properties.group.id' = 'testGroup-2',\n" +
//                "  'scan.startup.mode' = 'timestamp',\n" +
//                "  'scan.startup.timestamp-millis' = '%d',\n" +
//                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
//                "  'format' = 'dlink-json' -- required: Kafka connector requires to specify a format,\n" +
//                ")",DaySurveyTopic,kafkaServers,kafkaStartMills);
//        tableEnv.executeSql(sql);
        String sql=String.format("CREATE TABLE NcovLimsurvey (\n" +
                " Eid int NULL,\n" +
                " TotalEmpHealthStatus string NULL,\n"+
                " CreateDate timestamp,\n" +
                " IsValid int,\n" +
                " Q51 string\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'dlink-json.ignore-parse-errors' = 'false',\n" +
                "  'topic' = '%s', -- required: topic name from which the table is read\n" +
                "  -- required: specify the Kafka server connection string\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
                "  'properties.group.id' = 'testGroup-3',\n" +
                "  'scan.startup.mode' = 'timestamp',\n" +
                "  'scan.startup.timestamp-millis' = '%d',\n" +
                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
                "  'format' = 'dlink-json' -- required: Kafka connector requires to specify a format,\n" +
                ")",NcovLimsurveyTopic,kafkaServers,kafkaStartMills);
        tableEnv.executeSql(sql);
//        org.apache.flink.table.api.Table AkmResultTable=tableEnv.sqlQuery(String.format("select * from AkmResult where CreateDate>='%s'",CreateDate));
//        DataStream<Tuple2<Boolean,Row>> AkmResultStream = tableEnv.toRetractStream(AkmResultTable, Row.class);
//        FlatMapFunction mapFunction=new HealthFlatMapFunction("安康码",corpid,corpsecret,toUsers,dbServerUrl,dbServerUserName,dbServerPasswd,dbServerReloadInterval);
//        SingleOutputStreamOperator<Row> AkmResultFlatMapStream =AkmResultStream.map(v->v.f1).startNewChain().
//                filter(r-> r.getField(1) !=null && (r.getField(1).equals("3")||r.getField(1).equals("2"))).
//                flatMap(mapFunction);
//
//        org.apache.flink.table.api.Table NCOV_DaySurveyResult=tableEnv.sqlQuery(String.format("select * from NCOV_DaySurveyResult where CreateDate>='%s'",CreateDate));
//        DataStream<Tuple2<Boolean,Row>> NCOV_DaySurveyResultStream=tableEnv.toRetractStream(NCOV_DaySurveyResult, Row.class);
//        mapFunction=new HealthFlatMapFunction("综合码",corpid,corpsecret,toUsers,dbServerUrl,dbServerUserName,dbServerPasswd,dbServerReloadInterval);
//        SingleOutputStreamOperator<Row> NCOV_DaySurveyResultFlatMapStream =NCOV_DaySurveyResultStream.map(v->v.f1).startNewChain().
//                filter(r-> r.getField(1) !=null && (r.getField(1).equals("3")||r.getField(1).equals("2"))).
//                flatMap(mapFunction);

        org.apache.flink.table.api.Table NcovLimsurveyResult=tableEnv.sqlQuery(String.format("select * from NcovLimsurvey where CreateDate>='%s'",CreateDate));
        DataStream<Tuple2<Boolean,Row>> NcovLimsurveyResultStream=tableEnv.toRetractStream(NcovLimsurveyResult, Row.class);
        FlatMapFunction NcovLimsurveyFlatMapFunction=new HealthFlatMapFunction("防疫问卷信息",corpid,corpsecret,toUsers,dbServerUrl,dbServerUserName,dbServerPasswd,dbServerReloadInterval);
        FilterFunction<Row> ff = new FilterFunction<Row>(){

            @Override
            public boolean filter(Row r) {
                return (r.getField(4) !=null && r.getField(4).toString().startsWith("是")) && (r.getField(3) !=null && ((int)r.getField(3)) == 1);
            }
        };

        SingleOutputStreamOperator<Row> NcovLimsurveyResultFlatMapStream =NcovLimsurveyResultStream.map(v->v.f1).startNewChain().
                filter(ff).
                flatMap(NcovLimsurveyFlatMapFunction);

        //DataStream<Row> unionSinkStream=AkmResultFlatMapStream.union(NCOV_DaySurveyResultFlatMapStream).union(NcovLimsurveyResultFlatMapStream);//.transform("SysEmployeeToRow", TypeInformation.of(Row.class), new IcebergSinkOperator());//.timeWindowAll(Time.minutes(1)).process(new IcebergSinkAllWindow<SysEmployee>());
        DataStream<Row> unionSinkStream=NcovLimsurveyResultFlatMapStream;
        TableSchema icebergTableSchema=AlertEmploySchema.AlertEmployTableSchema();
        CatalogUtil.reCreateIcebergTableIfExist(tenantId, projId, catalogName, databaseName, icebergTableName, icebergTableSchema, Lists.newArrayList(), new HashMap<>(), "table for parsed row data", false, true);
        String catalogMappingName = CatalogUtil.icebergCatalog.getCatalogMappingName();
        TableIdentifier icebergTableIdentifier = TableIdentifier.of(databaseName, icebergTableName);
        CatalogLoader catalogLoader = CatalogUtil.getHiveCatalogLoader(catalogMappingName, warehouse,thriftUri);
        TableLoader AkmResultTableLoader = TableLoader.fromCatalog(catalogLoader, icebergTableIdentifier);
        FlinkSink.forRow(unionSinkStream, icebergTableSchema)
                .tableLoader(AkmResultTableLoader)
                .tableSchema(icebergTableSchema)
                .build();
        env.execute(HealthCodeFlinkApp.class.getName());
    }
}
