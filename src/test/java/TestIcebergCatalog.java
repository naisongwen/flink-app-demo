import com.backend.faas.catalogmanager.CatalogManager;
import com.backend.faas.catalogmanager.catalog.DlinkCatalog;
import com.backend.faas.catalogmanager.catalog.DlinkIcebergCatalog;
import com.backend.faas.catalogmanager.common.DlinkCatalogBuilder;
import com.backend.faas.catalogmanager.enums.CatalogTypeEnum;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

import org.junit.Before;
import org.junit.Test;
import shaded.parquet.org.apache.thrift.TException;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergCatalog {
    public final String warehouse = "hdfs://10.201.0.82:9000/dlink_test/catalogmanager/test/";
    String thriftUri = "thrift://10.201.0.44:9083";

    String tenantId = "980145681563910144";
    String catalogName = "iceberg_default";
    String databaseName = "test_yy";
    String tableName = "dic_json_quanliang";
    Integer projId = 403;
    boolean externalCatalog = false;

    String catalogNameMap;

    DlinkIcebergCatalog dlinkIcebergCatalog;

    public DlinkIcebergCatalog buildIcebergCatalog() {
        DlinkIcebergCatalog icebergCatalog = (DlinkIcebergCatalog) DlinkCatalogBuilder.builder()
                .withCatalogName(catalogName)
                .withTenantId(tenantId).withProjectId(projId).withIsExternalCatalog(externalCatalog)
                .withCatalogType(CatalogTypeEnum.ICEBERG_DEFAULT)
                .build();

        dlinkIcebergCatalog = icebergCatalog;
        catalogNameMap = icebergCatalog.getCatalogMappingName();
        return icebergCatalog;
    }

    @Before
    public void init() {
        System.setProperty("catalog.config", "file://" + new File("src/main/resources/catalog.properties").getAbsolutePath());
        String config = System.getProperty("catalog.config");
//        CatalogManager catalogManager = new CatalogManager();
//        catalogManager.newCatalog(tenantId,projId,catalogName,null,CatalogTypeEnum.ICEBERG_DEFAULT,externalCatalog);
        buildIcebergCatalog();
    }

    @Test
    public void listCatalogs() {
        CatalogManager catalogManager = new CatalogManager();
        List<DlinkCatalog> catalogs = catalogManager.listCatalogs(tenantId, projId, CatalogTypeEnum.ICEBERG_DEFAULT, externalCatalog);
        assert catalogs.size() != 0;
    }

    @Test
    public void createDatabaseTest() throws DatabaseAlreadyExistException {
        dlinkIcebergCatalog.createDatabase(databaseName, null, false);
    }

    @Test
    public void dropTableTest() throws TableNotExistException {
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        dlinkIcebergCatalog.dropTable(tablePath, true);
    }

    @Test
    public void listDatabaseTest() {
        List<String> strings = dlinkIcebergCatalog.listDatabases();
        assert strings.size() > 0;
    }

    @Test
    public void createTableTest() throws TableAlreadyExistException, DatabaseNotExistException {
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("name", DataTypes.STRING()).build();
        Map<String, String> properties = new HashMap<>();
        String comment = "zj test";
        CatalogBaseTable table = new CatalogTableImpl(tableSchema, properties, comment);
        dlinkIcebergCatalog.createTable(tablePath, table, false);
    }

    @Test
    public void listTableTest() throws DatabaseNotExistException {
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        List<String> tableList = dlinkIcebergCatalog.listTables(databaseName);
        System.out.println(tableList);
    }

    @Test
    public void testLoadTable() {
        CatalogLoader catalogLoader = CatalogUtil.getHiveCatalogLoader(catalogNameMap, warehouse, thriftUri);
        Catalog catalog = catalogLoader.loadCatalog();
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(databaseName));
        TableIdentifier dataIdentifier = TableIdentifier.of(Namespace.of(databaseName), tableName);
        org.apache.iceberg.Table table = catalog.loadTable(dataIdentifier);
        assert table != null;
    }

    @Test
    public void testCatalogLoader() {
        Schema SCHEMA = new Schema(
                required(1, "id", Types.IntegerType.get()),
                required(2, "data", Types.StringType.get())
        );
//        catalog.createTable(dataIdentifier, SCHEMA);

        Map<String, String> properties = ImmutableMap
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hive")
                .of("uri", thriftUri);
//                .of("warehouse", "hdfs://10.201.0.82:9000/afei/catalogmanager/test_iceberg_catalog_304");

        Configuration cfg = new Configuration();
        cfg.set("datanucleus.schema.autoCreateTables", "true");
        cfg.set("hive.metastore.schema.verification", "false");
        cfg.set("metastore.catalog.default", catalogNameMap);
//        cfg.set("warehouse", "hdfs://10.201.0.82:9000/afei/catalogmanager/test_iceberg_catalog_304");
        CatalogLoader catalogLoader = CatalogLoader.hive(catalogNameMap, cfg, properties);
        Catalog catalog = catalogLoader.loadCatalog();
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(databaseName));
        TableIdentifier dataIdentifier = TableIdentifier.of(Namespace.of(databaseName), tableName);
        Table table = catalog.loadTable(dataIdentifier);
        assert table != null;
    }


    @Test
    public void testQueryHMS() throws TException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", thriftUri);
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        // 当前版本2.3.4与集群3.0版本不兼容，加入此设置
        hiveMetaStoreClient.setMetaConf("hive.metastore.client.capability.check", "false");
        try {
            List<String> catalogList = hiveMetaStoreClient.getCatalogs();
            List<String> dbList = hiveMetaStoreClient.getAllDatabases();
//            List<String>dbList=hiveMetaStoreClient.getTable();
            assert dbList != null;

        } catch (AlreadyExistsException e) {

        }
    }

    @Test
    public void testFlinkQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);

        tableEnvironment.registerCatalog(dlinkIcebergCatalog.getName(), dlinkIcebergCatalog);
        tableEnvironment.useCatalog(dlinkIcebergCatalog.getName());
        tableEnvironment.useDatabase("default");
        tableEnvironment.executeSql("select successEventCount,__processing_time__ from `iceberg_default`.`default`.layuetest01_stat").print();
        env.execute();
    }
}
