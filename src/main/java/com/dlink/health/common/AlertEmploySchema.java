package com.dlink.health.common;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.LinkedHashMap;
import java.util.Map;

public class AlertEmploySchema {

    public static TableSchema AlertEmployTableSchema() {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        return schemaBuilder
                .field("badge", DataTypes.STRING())
                .field("Name", DataTypes.STRING())
                .field("CodeType", DataTypes.STRING())
                .field("HealthStatus", DataTypes.STRING())
                .field("AlertDate", DataTypes.DATE()).build();
    }

    public static TableSchema NcovLimsurveyTableSchema() {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        return schemaBuilder
                .field("Q1", DataTypes.STRING())
                .field("Q3", DataTypes.STRING())
                .field("vname", DataTypes.STRING())
                .field("CodeType", DataTypes.STRING())
                .field("Q74", DataTypes.STRING())
                .field("AlertDate", DataTypes.DATE()).build();
    }
}
