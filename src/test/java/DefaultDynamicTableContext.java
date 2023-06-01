import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableFactory;

public class DefaultDynamicTableContext implements DynamicTableFactory.Context {

    private final ObjectIdentifier objectIdentifier;
    private final CatalogTable catalogTable;
    private final ReadableConfig configuration;
    private final ClassLoader classLoader;
    private final boolean isTemporary;

    DefaultDynamicTableContext(
            ObjectIdentifier objectIdentifier,
            CatalogTable catalogTable,
            ReadableConfig configuration,
            ClassLoader classLoader,
            boolean isTemporary) {
        this.objectIdentifier = objectIdentifier;
        this.catalogTable = catalogTable;
        this.configuration = configuration;
        this.classLoader = classLoader;
        this.isTemporary = isTemporary;
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
        return objectIdentifier;
    }

    @Override
    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return configuration;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public boolean isTemporary() {
        return isTemporary;
    }
}
