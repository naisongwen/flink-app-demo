import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class TestJdbcLookup {
    String DBURL="jdbc:postgresql://10.201.0.212:5432/postgres";
    String LOOKUP_TABLE="testTable";
    @Test
    public void testLookup(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "3"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "5"),
                                        new Tuple2<>(3, "8"))),
                        $("id1"),
                        $("id2"));

        tEnv.registerTable("T", t);
        JdbcTableSource.Builder builder =
                JdbcTableSource.builder()
                        .setOptions(
                                JdbcOptions.builder()
                                        .setDBUrl(DBURL)
                                        .setTableName(LOOKUP_TABLE)
                                        .build())
                        .setSchema(
                                TableSchema.builder()
                                        .fields(
                                                new String[] {"id", "name"},
                                                new DataType[] {
                                                        DataTypes.INT(),
                                                        DataTypes.STRING()
                                                })
                                        .build());
        if (false) {
            builder.setLookupOptions(
                    JdbcLookupOptions.builder()
                            .setCacheMaxSize(1000)
                            .setCacheExpireMs(1000 * 1000)
                            .build());
        }
        tEnv.registerFunction(
                "jdbcLookup", builder.build().getLookupFunction(new String[] {"id"}));

        // do not use the first N fields as lookup keys for better coverage
        String sqlQuery =
                "SELECT id1, id2, name FROM T, "
                        + "LATERAL TABLE(jdbcLookup(id1)) AS S(id, name)";
        tEnv.executeSql(sqlQuery).collect();
    }
}
