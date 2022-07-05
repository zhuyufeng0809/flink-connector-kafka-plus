package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.*;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class KafkaCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalog.class);
    private static final String QUERY_TOPIC_NAME = "select distinct concat(source_connector, '.', source_db_name, '.', table_name)\n" +
            "from zion.flink_consumer_sub_task_config\n" +
            "where is_del = 0\n" +
            "  and concat(sink_connector, '_', sink_db_name, '_', table_name) = '%s'\n" +
            "limit 1;";
    private static final List<String> EXCLUDE_COLUMN = Stream.of("etl_time", "is_del", "binlog_file", "pos").collect(Collectors.toList());;

    private final String username;
    private final String password;
    private final String url;
    private final String bootstrapServers;

    public KafkaCatalog(String name, String defaultDatabase, String username, String password, String url, String bootstrapServers) {
        super(name, defaultDatabase);
        this.username = username;
        this.password = password;
        this.url = url;
        this.bootstrapServers = bootstrapServers;
    }

    private Optional<String> getTopicName(String tableName) {
        String result = null;

        try(Connection conn = DriverManager.getConnection(url, username, password)) {
            PreparedStatement statement = conn.prepareStatement(String.format(QUERY_TOPIC_NAME, tableName));
            ResultSet resultSet = statement.executeQuery();

            resultSet.next();
            result = resultSet.getString(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(result);
    }

    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        //reference Flink official website
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/jdbc/#data-type-mapping
        switch (mysqlType) {
            case MysqlDataType.MYSQL_TINYINT:
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case MysqlDataType.MYSQL_SMALLINT:
            case MysqlDataType.MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MysqlDataType.MYSQL_INT:
            case MysqlDataType.MYSQL_MEDIUMINT:
            case MysqlDataType.MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MysqlDataType.MYSQL_BIGINT:
            case MysqlDataType.MYSQL_INT_UNSIGNED:
                return DataTypes.BIGINT();
            case MysqlDataType.MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MysqlDataType.MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MysqlDataType.MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MysqlDataType.MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MysqlDataType.MYSQL_BOOLEAN:
                return DataTypes.BOOLEAN();
            case MysqlDataType.MYSQL_DATE:
                return DataTypes.DATE();
            case MysqlDataType.MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MysqlDataType.MYSQL_DATETIME:
            case MysqlDataType.MYSQL_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case MysqlDataType.MYSQL_CHAR:
            case MysqlDataType.MYSQL_VARCHAR:
            case MysqlDataType.MYSQL_TEXT:
                //type 'BIT' reference PolarDb Mysql official website
                //https://help.aliyun.com/document_detail/131282.htm?spm=a2c4g.11186623.0.0.4f86739cjdzqAQ#concept-1813784
            case MysqlDataType.MYSQL_BIT:
                return DataTypes.STRING();
            case MysqlDataType.MYSQL_BINARY:
            case MysqlDataType.MYSQL_VARBINARY:
            case MysqlDataType.MYSQL_BLOB:
                return DataTypes.BYTES();
            default:
                String columnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Mysql type '%s' of column '%s' yet, please contact author", mysqlType,columnName));
        }
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Catalog {} starting", getName());
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", getName());
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        Optional<String> topicName = getTopicName(tablePath.getObjectName());
        if (!topicName.isPresent()) {
            throw new CatalogException(String.format("Failed getting topic name for table %s", tablePath.getObjectName()));
        }

        try(Connection conn = DriverManager.getConnection(url, username, password)) {
            PreparedStatement ps =
                    conn.prepareStatement(String.format("SELECT * FROM zion.%s;", tablePath.getObjectName()));

            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            Map<String, DataType> columns = new HashMap<>();

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String columnName = resultSetMetaData.getColumnName(i);

                if (EXCLUDE_COLUMN.contains(columnName)) {
                    continue;
                }

                columns.put(columnName, fromJDBCType(resultSetMetaData, i));
            }

            Schema tableSchema = Schema
                    .newBuilder()
                    .fromFields(columns.keySet().toArray(new String[0]), columns.values().toArray(new DataType[0]))
                    .build();

            Map<String, String> props = new HashMap<>();

            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(TOPIC.key(), topicName.get());
            props.put(PROPS_BOOTSTRAP_SERVERS.key(), bootstrapServers);
            props.put(VALUE_FORMAT.key(), "debezium-json");

            return new KafkaCatalogTable(tableSchema, props, null);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
