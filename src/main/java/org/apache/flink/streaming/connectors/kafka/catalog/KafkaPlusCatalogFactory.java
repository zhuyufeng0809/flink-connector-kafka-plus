package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.catalog.KafkaCatalogFactoryOptions.*;

public class KafkaPlusCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new KafkaCatalog(
                context.getName(),
                "default_database",
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(URL),
                helper.getOptions().get(BOOTSTRAP_SERVERS));
    }

    @Override
    public String factoryIdentifier() {
        return KafkaCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(URL);
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
