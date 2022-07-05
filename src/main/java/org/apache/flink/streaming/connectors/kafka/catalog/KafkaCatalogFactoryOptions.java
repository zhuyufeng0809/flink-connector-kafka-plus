package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

@Internal
public class KafkaCatalogFactoryOptions {
    public static final String IDENTIFIER = "kafka-plus";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue();

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers").stringType().noDefaultValue();

    private KafkaCatalogFactoryOptions() {}
}
