package com.dlink.health.dsg;

import com.dlink.health.common.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.eclipse.jetty.util.ajax.JSON;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.*;

import static com.dlink.health.common.TimeFormatUtil.*;

@Slf4j
public class DsgJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private static final String OP_INSERT = "I";
    private static final String OP_UPDATE = "U";
    private static final String OP_DELETE = "D";
    private static final String OP_DDL_ALTER = "L";
    private static final String JSON_OP_IDENTFIER = "operationType";
    private static final String JSON_BEFORECOLUMNLIST_IDENTFIER = "beforeColumnList";
    private static final String JSON_AFTERCOLUMNLIST_IDENTFIER = "afterColumnList";
    private final boolean ignoreParseErrors;
    private final boolean failOnMissingField = false;
    private final TimestampFormat timestampFormat;
    private final TypeInformation<RowData> resultTypeInfo;
    private String encoding = "UTF8";
    private Map<String, DsgDeserializationRuntimeConverter> converterMap;
    private List<String> schemaFields;

    DsgJsonDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {
        this.resultTypeInfo = resultTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormatOption;
        List<String> fieldNames = rowType.getFieldNames();
        List<RowType.RowField> fields = rowType.getFields();
        this.converterMap = new HashMap();

        for (int i = 0; i < fieldNames.size(); ++i) {
            this.converterMap.put(((String) fieldNames.get(i)).toLowerCase(), this.createConverter(((RowType.RowField) fields.get(i)).getType()));
        }

        this.schemaFields = new ArrayList();
        Iterator var9 = rowType.getFieldNames().iterator();

        while (var9.hasNext()) {
            String fieldName = (String) var9.next();
            this.schemaFields.add(fieldName.toLowerCase());
        }

        if (log.isDebugEnabled()) {
            log.debug("********************************* converterMap : " + this.converterMap);
        }
    }

    @Override
    public void open(InitializationContext context) {
    }

    @Override
    public RowData deserialize(byte[] bytes) {
        throw new RuntimeException("Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) {
        String content = null;
        try {
            content = new String(message, encoding);
            content= StringUtil.formatErrorJson(content);
            Map<String, Object> obj = (Map<String, Object>) JSON.parse(content);
            String op = obj.get("operationType").toString();
            Map<String, Object> afterColumnList = (Map<String, Object>) obj.get("afterColumnList");
            Map<String, Object> beforeColumnList = (Map<String, Object>) obj.get("beforeColumnList");
            Map<String, Object> newBeforeColumnList = Maps.newHashMap();
            if(beforeColumnList !=null) {
                for (Map.Entry<String, Object> entry : beforeColumnList.entrySet()) {
                    newBeforeColumnList.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
                }
            }
            Map<String, Object> newAfterColumnList = Maps.newHashMap();
            if(afterColumnList !=null) {
                for (Map.Entry<String, Object> entry : afterColumnList.entrySet()) {
                    newAfterColumnList.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
                }
            }
            GenericRowData beforeRowData = null;
            if (beforeColumnList != null) {
                beforeRowData = this.generateGenericRowData(newBeforeColumnList);
            }

            GenericRowData afterRowData = null;
            if (afterColumnList != null) {
                afterRowData = this.generateGenericRowData(newAfterColumnList);
            }

            if ("I".equalsIgnoreCase(op)) {
                afterRowData.setRowKind(RowKind.INSERT);
                out.collect(afterRowData);
            } else if ("U".equalsIgnoreCase(op)) {
                beforeRowData.setRowKind(RowKind.UPDATE_BEFORE);
                out.collect(beforeRowData);
                afterRowData.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(afterRowData);
            } else if ("D".equalsIgnoreCase(op)) {
                afterRowData.setRowKind(RowKind.DELETE);
                out.collect(afterRowData);
            } else if ("L".equalsIgnoreCase(op)) {
                log.warn("alter table not support,ignore op:" + op + ",msg:" + content);
            } else {
                log.warn("ignore op:" + op + ",msg:" + content);
                if (!this.ignoreParseErrors) {
                    throw new IllegalArgumentException(String.format("Unknown \"op\" value \"%s\". The DSG JSON message is '%s'", op, content));
                }
            }
        } catch (Throwable t) {
            log.error(String.format("dsg deserialize error,src content :%s", content), t);
            if (!this.ignoreParseErrors) {
                throw new IllegalArgumentException(String.format("Corrupt DSG JSON message '%s'.", content), t);
            }
        }

    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    private DsgDeserializationRuntimeConverter createConverter(LogicalType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private DsgDeserializationRuntimeConverter wrapIntoNullableConverter(DsgDeserializationRuntimeConverter converter) {
        return (obj) -> {
            if (obj != null) {
                try {
                    return converter.convert(obj);
                } catch (Throwable var4) {
                    if (!this.ignoreParseErrors) {
                        throw var4;
                    } else {
                        return null;
                    }
                }
            } else {
                return null;
            }
        };
    }

    private DsgDeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (object) -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return (object) -> Byte.parseByte(object.toString().trim());
            case SMALLINT:
                return (object) -> {
                    return Short.parseShort(object.toString().trim());
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return this.createDecimalConverter((DecimalType) type);
            case ARRAY:
                return this.createArrayConverter((ArrayType) type);
            case MAP:
            case MULTISET:
                return this.createMapConverter((MapType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(Object obj) {
        return obj instanceof Boolean ? (Boolean) obj : Boolean.parseBoolean(obj.toString().trim());
    }

    private int convertToInt(Object obj) {
        return obj instanceof Integer ? (Integer) obj : Integer.parseInt(obj.toString().trim());
    }

    private long convertToLong(Object obj) {
        return obj instanceof Long ? (Integer) obj : Long.parseLong(obj.toString().trim());
    }

    private double convertToDouble(Object obj) {
        return obj instanceof Double ? (Double) obj : Double.parseDouble(obj.toString().trim());
    }

    private float convertToFloat(Object obj) {
        return obj instanceof Float ? (Float) obj : Float.parseFloat(obj.toString().trim());
    }

    private int convertToDate(Object obj) {
        LocalDate date = DateTimeFormatter.ISO_LOCAL_DATE.parse(obj.toString().trim()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(Object obj) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(obj.toString().trim());
        LocalTime localTime = (LocalTime) parsedTime.query(TemporalQueries.localTime());
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(Object obj) {
        Object parsedTimestamp;
        if (obj instanceof Long) {
            long timestamp = (long) obj;
            parsedTimestamp = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        } else {
            switch (this.timestampFormat) {
                case SQL:
                    parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(obj.toString().trim());
                    break;
                case ISO_8601:
                    parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(obj.toString().trim());
                    break;
                default:
                    throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", this.timestampFormat));
            }
        }

        LocalTime localTime = ((TemporalAccessor) parsedTimestamp).query(TemporalQueries.localTime());
        LocalDate localDate = ((TemporalAccessor) parsedTimestamp).query(TemporalQueries.localDate());
        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private StringData convertToString(Object obj) {
        return StringData.fromString(obj.toString().trim());
    }

    private byte[] convertToBytes(Object obj) {
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    private DsgDeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        return (obj) -> {
            BigDecimal bigDecimal;
            if (obj instanceof BigDecimal) {
                bigDecimal = (BigDecimal) obj;
            } else {
                bigDecimal = new BigDecimal(obj.toString().trim());
            }

            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private DsgDeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
        DsgDeserializationRuntimeConverter elementConverter = this.createConverter(arrayType.getElementType());
        Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return (jsonNode) -> {
            ArrayNode node = (ArrayNode) jsonNode;
            Object[] array = (Object[]) ((Object[]) Array.newInstance(elementClass, node.size()));

            for (int i = 0; i < node.size(); ++i) {
                JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }

            return new GenericArrayData(array);
        };
    }

    private DsgDeserializationRuntimeConverter createMapConverter(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException("JSON format doesn't support non-string as key type of map. The map type is: " + mapType.asSummaryString());
        } else {
            DsgDeserializationRuntimeConverter keyConverter = this.createConverter(keyType);
            DsgDeserializationRuntimeConverter valueConverter = this.createConverter(mapType.getValueType());
            return (obj) -> {
                Iterator<Map.Entry<String, Object>> fields = ((Map)obj).entrySet().iterator();
                HashMap<String,Object> result = new HashMap();
                while (fields.hasNext()) {
                    Map.Entry entry = fields.next();
                    String key = (String) keyConverter.convert(entry.getKey());
                    Object value = valueConverter.convert(entry.getValue());
                    result.put(key, value);
                }
                return new GenericMapData(result);
            };
        }
    }

    private Object convertField(DsgDeserializationRuntimeConverter fieldConverter, String fieldName, Object field) {
        return field == null ? null : fieldConverter.convert(field);
    }

    private GenericRowData generateGenericRowData(Map<String, Object> map) {
        GenericRowData rowData = new GenericRowData(this.schemaFields.size());
        for (int j = 0; j < this.schemaFields.size(); ++j) {
            String fieldName = this.schemaFields.get(j);
            Object field = map.get(fieldName);
            Object convertedField = null;
            if (field != null) {
                convertedField = this.convertField(this.converterMap.get(fieldName.toLowerCase()), fieldName, field);
            }
            rowData.setField(j, convertedField);
        }
        return rowData;
    }

    @FunctionalInterface
    private interface DsgDeserializationRuntimeConverter extends Serializable {
        Object convert(Object var1);
    }
}
