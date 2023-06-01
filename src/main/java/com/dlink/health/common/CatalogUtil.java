package com.dlink.health.common;

import com.backend.faas.catalogmanager.catalog.DlinkIcebergCatalog;
import com.backend.faas.catalogmanager.common.DlinkCatalogBuilder;
import com.backend.faas.catalogmanager.enums.CatalogTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @author wennaisong
 */
@Slf4j
public class CatalogUtil {

    public static DlinkIcebergCatalog icebergCatalog = null;

    public static Table getOrCreateIcebergTable(CatalogLoader catalogLoader, TableIdentifier identifier, Schema schema, boolean cleanWarehouse) throws IOException, URISyntaxException {
        Table table = null;
        Catalog catalog = catalogLoader.loadCatalog();
        try {
            //NOTE:make sure the database already exists
            table = catalog.loadTable(identifier);
            if (!cleanWarehouse) {
                return table;
            }
            //NOTE:make sure table dropped before deleting data
            catalog.dropTable(identifier);
        } catch (Exception e) {
            catalog.dropTable(identifier, false);
//            if (table != null) {
//                String location = table.location();
//                FileSystem fs = FileSystem.get(new URI(defaultFS), getHiveConf());
//                fs.delete(new Path(location), true);
//            }
            log.error("dropTable error", e);
        }
        table = catalog.createTable(identifier, schema);//, null, warehouse, null);
        return table;
    }

    static Configuration getHiveConf(String catalogName, String defaultFS) {
        Configuration cfg = new Configuration();
        cfg.set("metastore.catalog.default", catalogName);
        return cfg;
    }

    public static CatalogLoader getHiveCatalogLoader(String catalogName, String warehouse, String thriftUri) {
        CatalogLoader catalogLoader;
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hive")
                .of("warehouse", warehouse)
                .of("uri", thriftUri);

        catalogLoader = CatalogLoader.hive(catalogName, getHiveConf(catalogName, warehouse), properties);
        return catalogLoader;
    }

    public static void reCreateIcebergTableIfExist(String tenantId, Integer projectId, String catalogName, String databaseName, String tableName, TableSchema tableSchema, List<String> partitionkeys, Map<String, String> properties, String comment, boolean cleanTableAndDataIfExists, boolean isExternalCatalog) throws DatabaseNotExistException, TableAlreadyExistException {
        icebergCatalog = (DlinkIcebergCatalog) DlinkCatalogBuilder.builder()
                .withCatalogName(catalogName)
                .withTenantId(tenantId).withProjectId(projectId).withIsExternalCatalog(isExternalCatalog)
                .withCatalogType(CatalogTypeEnum.ICEBERG_DEFAULT)
                .build();

        Preconditions.checkNotNull(icebergCatalog);
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        CatalogBaseTable table = new CatalogTableImpl(tableSchema, partitionkeys, properties, comment);
        if (icebergCatalog.tableExists(tablePath)) {
            if (cleanTableAndDataIfExists) {
                try {
                    icebergCatalog.dropTable(tablePath, true);
                } catch (TableNotExistException x) {

                }
                //重建表
                icebergCatalog.createTable(tablePath, table, false);
            }
        } else {
            icebergCatalog.createTable(tablePath, table, false);
        }
    }
}
