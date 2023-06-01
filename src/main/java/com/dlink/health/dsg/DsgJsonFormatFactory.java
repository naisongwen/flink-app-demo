package com.dlink.health.dsg;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DsgJsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    private static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS;
    private static final ConfigOption<String> TIMESTAMP_FORMAT;

    public DsgJsonFormatFactory() {
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final TimestampFormat timestampFormatOption = JsonOptions.getTimestampFormat(formatOptions);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.UPDATE_BEFORE).addContainedKind(RowKind.UPDATE_AFTER).addContainedKind(RowKind.DELETE).build();
            }

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
                RowType rowType = (RowType)producedDataType.getLogicalType();
                TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(producedDataType);
                return new DsgJsonDeserializationSchema(rowType, rowDataTypeInfo, ignoreParseErrors, timestampFormatOption);
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return "dlink-json";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        throw new UnsupportedOperationException("not support as a sink format yet.");
    }

    static {
        IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
        TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
    }
}
