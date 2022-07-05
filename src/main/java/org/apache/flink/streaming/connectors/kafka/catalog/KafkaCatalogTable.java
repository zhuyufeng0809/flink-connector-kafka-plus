package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.*;

public class KafkaCatalogTable extends AbstractCatalogTable {

    public KafkaCatalogTable(Schema tableSchema, Map<String, String> options, String comment) {
        super(tableSchema, options, comment);
    }

    public KafkaCatalogTable(Schema tableSchema, List<String> partitionKeys, Map<String, String> options, String comment) {
        super(tableSchema, partitionKeys, options, comment);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new KafkaCatalogTable(getUnresolvedSchema(), getPartitionKeys(), options, getComment());
    }

    @Override
    public CatalogBaseTable copy() {
        Schema schema = Schema.newBuilder().fromSchema(getUnresolvedSchema()).build();

        return new KafkaCatalogTable(
                schema,
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getComment());
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a catalog table in an im-memory catalog");
    }
}
