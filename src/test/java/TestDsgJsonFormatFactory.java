import com.dlink.health.dsg.DsgJsonFormatFactory;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

//TestFileSourceFactory
public class TestDsgJsonFormatFactory {
    @Test
    public void testDeserial() throws IOException {
        String value="{\"array\":[1,2,3],\"owner\":\"USER01\",\"tableName\":\" TEST01\",\"operationType\":\"D\",\"rowNum\":\"2\",\"columnNum\":\"10\",\"opTs\":\"2018-02-02 20:21:25\",\"scn\":\"56224332\",\"seqid\":\"1\",\"tranid\":\"5911099065018442\",\"loaderTime\":\"2018-02-02 20:21:29\",\"rowid\":\"AAApMdAAHAAANpfAAH\",\"afterColumnList\":{\"ID\":\"2\",\"NAME\":\"11zjjjj\",\"HDATE\":\"2018-02-01 22:23:48\"}}";
        byte[] message =value.getBytes(StandardCharsets.UTF_8);
        DsgJsonFormatFactory factory=new DsgJsonFormatFactory();
        //FactoryUtil
        ObjectIdentifier objectIdentifier=ObjectIdentifier.of("default_catalog","default_database","default_tbl");
        TableSchema tableSchema=TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("NAME", DataTypes.STRING())
                .build();
        DataType physicalDataType =
                tableSchema.toPhysicalRowDataType();
        CatalogTable catalogTable=new CatalogTableImpl(tableSchema,Maps.newHashMap(),"");
        Configuration conf=Configuration.fromMap(Maps.newHashMap());

        ReadableConfig formatOptions=new DelegatingConfiguration(conf, "");
        DefaultDynamicTableContext defaultDynamicTableContext = new DefaultDynamicTableContext(
                        objectIdentifier, catalogTable, formatOptions, TestDsgJsonFormatFactory.class.getClassLoader(), false);
        DynamicTableSource.Context context=ScanRuntimeProviderContext.INSTANCE;
//        final TypeInformation<RowData> producedTypeInfo =
//                context.createTypeInformation(producedDataType);
        //KafkaFetcher
        ListCollector listCollector=new ListCollector(Lists.newArrayList());
        DecodingFormat<DeserializationSchema<RowData>>  deserializationSchemaDecodingFormat=factory.createDecodingFormat(defaultDynamicTableContext,formatOptions);
        DeserializationSchema<RowData> dataDeserializationSchema=deserializationSchemaDecodingFormat.createRuntimeDecoder(context,physicalDataType);
        dataDeserializationSchema.deserialize(message,listCollector);
        System.out.println(listCollector);
    }
}
