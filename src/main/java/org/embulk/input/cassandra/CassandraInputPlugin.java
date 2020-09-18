package org.embulk.input.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.cassandra.writers.ColumnWriter;
import org.embulk.input.cassandra.writers.ColumnWriterFactory;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Schema.Builder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class CassandraInputPlugin implements InputPlugin
{
  private static Logger logger = LoggerFactory.getLogger(CassandraInputPlugin.class);

  public interface PluginTask extends Task
  {
    @Config("hosts")
    @ConfigDefault("[\"localhost\"]")
    List<String> getHosts();

    @Config("port")
    @ConfigDefault("9042")
    int getPort();

    @Config("username")
    @ConfigDefault("null")
    Optional<String> getUsername();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    @Config("cluster_name")
    @ConfigDefault("null")
    Optional<String> getClustername();

    @Config("concurrency")
    @ConfigDefault("null")
    Optional<Integer> getConcurrency();

    @Config("keyspace")
    String getKeyspace();

    @Config("table")
    String getTable();

    @Config("select")
    @ConfigDefault("[]")
    List<String> getSelect();

    @Config("filter_by_partition_keys")
    @ConfigDefault("{}")
    Map<String, Object> getFilterByPartitionKeys();

    @Config("connect_timeout")
    @ConfigDefault("5000")
    int getConnectTimeout();

    @Config("request_timeout")
    @ConfigDefault("12000")
    int getRequestTimeout();

    @Config("range_mappings")
    @ConfigDefault("[]")
    List<List<Long>> getRangeMappings();
    void setRangeMappings(List<List<Long>> mappings);
  }

  private Cluster getCluster(PluginTask task)
  {
    Cluster.Builder builder = Cluster.builder();
    for (String host : task.getHosts()) {
      builder.addContactPointsWithPorts(new InetSocketAddress(host, task.getPort()));
    }

    if (task.getUsername().isPresent()) {
      builder.withCredentials(task.getUsername().get(), task.getPassword().orElse(null));
    }

    if (task.getClustername().isPresent()) {
      builder.withClusterName(task.getClustername().get());
    }

    builder.withSocketOptions(
        new SocketOptions()
            .setConnectTimeoutMillis(task.getConnectTimeout())
            .setReadTimeoutMillis(task.getRequestTimeout()));

    return builder.build();
  }

  private TableMetadata getTableMetadata(Cluster cluster, PluginTask task)
  {
    return cluster.getMetadata().getKeyspace(task.getKeyspace()).getTable(task.getTable());
  }

  private static Type getEmbulkType(DataType cassandraDataType)
  {
    switch (cassandraDataType.getName()) {
      case INT:
      case BIGINT:
      case TINYINT:
      case VARINT:
      case SMALLINT:
      case COUNTER:
      case TIME:
        return Types.LONG;
      case TEXT:
      case VARCHAR:
      case ASCII:
      case UUID:
      case TIMEUUID:
      case INET:
      case DECIMAL:
        return Types.STRING;
      case DOUBLE:
      case FLOAT:
        return Types.DOUBLE;
      case DATE:
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case BOOLEAN:
        return Types.BOOLEAN;
      case MAP:
      case LIST:
      case SET:
        return Types.JSON;
      default:
        throw new RuntimeException("Unsupported cassandra data type");
    }
  }

  private List<List<Long>> splitTokenRange(int taskCount)
  {
    List<List<Long>> tokenRanges = new ArrayList<>();
    if (taskCount == 1) {
      List<Long> range = new ArrayList<>();
      range.add(Long.MIN_VALUE);
      range.add(Long.MAX_VALUE);
      tokenRanges.add(range);
    }
    else {
      BigInteger tokenMin = BigInteger.valueOf(Long.MIN_VALUE);
      BigInteger tokenMax = BigInteger.valueOf(Long.MAX_VALUE);
      long rangeSize = tokenMax.subtract(tokenMin).divide(BigInteger.valueOf(taskCount)).longValueExact();
      Long rangeStart = Long.MIN_VALUE;
      for (int i = 0; i < taskCount; i++) {
        List<Long> range = new ArrayList<>();
        range.add(rangeStart);
        if (i == taskCount - 1) {
          range.add(Long.MAX_VALUE);
        }
        else {
          range.add(rangeStart + rangeSize - 1);
        }
        tokenRanges.add(range);
        rangeStart = rangeStart + rangeSize;
      }
    }

    return tokenRanges;
  }

  @Override
  public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
  {
    PluginTask task = config.loadConfig(PluginTask.class);

    TableMetadata tableMetadata;
    try (Cluster cluster = getCluster(task)) {
      tableMetadata = getTableMetadata(cluster, task);
    }
    Builder schemaBuilder = Schema.builder();
    tableMetadata
        .getColumns()
        .forEach(
            columnMetadata -> {
              if (task.getSelect().isEmpty()) {
                schemaBuilder.add(
                    columnMetadata.getName(), getEmbulkType(columnMetadata.getType()));
              }
              else {
                if (task.getSelect().contains(columnMetadata.getName())) {
                  schemaBuilder
                      .add(columnMetadata.getName(), getEmbulkType(columnMetadata.getType()));
                }
              }
            });
    Schema schema = schemaBuilder.build();

    int taskCount = task.getConcurrency().orElse(Runtime.getRuntime().availableProcessors());

    List<List<Long>> tokenRanges = splitTokenRange(taskCount);
    task.setRangeMappings(tokenRanges);

    return resume(task.dump(), schema, taskCount, control);
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
  {
    control.run(taskSource, schema, taskCount);
    return Exec.newConfigDiff();
  }

  @Override
  public void cleanup(
      TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
  {
  }

  private Statement buildStatement(
      List<ColumnMetadata> partitionKeys,
      PluginTask task,
      int taskIndex)
  {
    Selection select = QueryBuilder.select();
    if (task.getSelect().isEmpty()) {
      select.all();
    }
    else {
      task.getSelect().forEach(select::column);
    }

    String[] partitionKeyNames = partitionKeys.stream().map(ColumnMetadata::getName).toArray(String[]::new);
    List<Long> tokenRange = task.getRangeMappings().get(taskIndex);
    Where where = select.from(task.getKeyspace(), task.getTable()).where();
    where.and(QueryBuilder.gte(QueryBuilder.token(partitionKeyNames), tokenRange.get(0)))
        .and(QueryBuilder.lt(QueryBuilder.token(partitionKeyNames), tokenRange.get(1)));

    task.getFilterByPartitionKeys().forEach((key, value) -> {
      if (value instanceof List) {
        @SuppressWarnings("unchecked")
        Object[] inValues = ((List<Object>) value).toArray();
        where.and(QueryBuilder.in(key, inValues));
      }
      else {
        where.and(QueryBuilder.eq(key, value));
      }
    });


    Statement statement = where.setIdempotent(true);
    logger.debug(statement.toString());
    return statement;
  }

  List<ColumnWriter> buildWriters(Schema schema, List<ColumnMetadata> columnMetadatas)
  {
    ImmutableList.Builder<ColumnWriter> writersBuilder = ImmutableList.builder();
    schema
        .getColumns()
        .forEach(
            column -> {
              int index = column.getIndex();
              columnMetadatas.stream()
                  .filter(metadata -> metadata.getName().equals(column.getName()))
                  .findFirst()
                  .ifPresent(
                      metadata -> writersBuilder.add(ColumnWriterFactory.get(index, metadata.getType())));
            });
    return writersBuilder.build();
  }

  @Override
  public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
  {
    PluginTask task = taskSource.loadTask(PluginTask.class);
    final Object lock = new Object();

    Cluster cluster = getCluster(task);
    try (Session session = cluster.newSession()) {
      TableMetadata tableMetadata = getTableMetadata(cluster, task);
      List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
      List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
      List<ColumnWriter> writers = buildWriters(schema, columnMetadatas);

      Statement statement = buildStatement(partitionKeys, task, taskIndex);

      BufferAllocator allocator = Exec.getBufferAllocator();
      try (PageBuilder pageBuilder = new PageBuilder(allocator, schema, output)) {
        ResultSetFuture future = session.executeAsync(statement);
        @SuppressWarnings("UnstableApiUsage")
        ListenableFuture<ResultSet> transform =
            Futures.transform(future, new AsyncPaging(pageBuilder, writers, lock));
        try {
          transform.get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }

        pageBuilder.finish();
      }
    }

    return Exec.newTaskReport();
  }

  @Override
  public ConfigDiff guess(ConfigSource config)
  {
    return Exec.newConfigDiff();
  }
}
