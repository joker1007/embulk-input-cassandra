package org.embulk.input.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
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

public class CassandraInputPlugin implements InputPlugin {
  private static Logger logger = LoggerFactory.getLogger(CassandraInputPlugin.class);

  public interface PluginTask extends Task {
    @Config("hosts")
    @ConfigDefault("[\"localhost\"]")
    public List<String> getHosts();

    @Config("port")
    @ConfigDefault("9042")
    public int getPort();

    @Config("username")
    @ConfigDefault("null")
    public Optional<String> getUsername();

    @Config("password")
    @ConfigDefault("null")
    public Optional<String> getPassword();

    @Config("cluster_name")
    @ConfigDefault("null")
    public Optional<String> getClustername();

    @Config("concurrency")
    @ConfigDefault("1")
    public Integer getConcurrency();

    @Config("keyspace")
    public String getKeyspace();

    @Config("table")
    public String getTable();

    @Config("select")
    @ConfigDefault("[]")
    public List<String> getSelect();

    @Config("where_clause_for_partition_key")
    @ConfigDefault("null")
    public Optional<String> getWhereClauseForPartitionKey();

    @Config("connect_timeout")
    @ConfigDefault("5000")
    public int getConnectTimeout();

    @Config("request_timeout")
    @ConfigDefault("12000")
    public int getRequestTimeout();

    // if you get schema from config
    //    @Config("columns")
    //    public SchemaConfig getColumns();

    @ConfigInject
    public BufferAllocator getBufferAllocator();
  }

  private Cluster getCluster(PluginTask task) {
    Cluster.Builder builder = Cluster.builder();
    for (String host : task.getHosts()) {
      builder.addContactPointsWithPorts(new InetSocketAddress(host, task.getPort()));
    }

    if (task.getUsername().isPresent()) {
      builder.withCredentials(task.getUsername().get(), task.getPassword().orNull());
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

  private TableMetadata getTableMetadata(Cluster cluster, PluginTask task) {
    return cluster.getMetadata().getKeyspace(task.getKeyspace()).getTable(task.getTable());
  }

  private static Type getEmbulkType(DataType cassandraDataType) {
    switch (cassandraDataType.getName()) {
      case INT:
      case BIGINT:
      case TINYINT:
      case VARINT:
      case SMALLINT:
      case COUNTER:
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

  @Override
  public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control) {
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
              } else {
                if (task.getSelect().contains(columnMetadata.getName())) {
                  schemaBuilder.add(
                      columnMetadata.getName(), getEmbulkType(columnMetadata.getType()));
                }
              }
            });
    Schema schema = schemaBuilder.build();

    int taskCount = task.getConcurrency();

    return resume(task.dump(), schema, taskCount, control);
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control) {
    control.run(taskSource, schema, taskCount);
    return Exec.newConfigDiff();
  }

  @Override
  public void cleanup(
      TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {}

  private ResultSet fetchPartitionKeys(
      Session session, String keyspace, String table, List<ColumnMetadata> partitionKeys) {
    Selection select = QueryBuilder.select();
    StringJoiner joiner = new StringJoiner(",");
    partitionKeys.forEach(
        key -> {
          select.column(key.getName());
          joiner.add(key.getName());
        });
    select.raw("token(" + joiner.toString() + ")");
    select.distinct();
    Select from = select.from(keyspace, table);

    System.out.println(from.getQueryString());

    return session.execute(from);
  }

  static class AsyncPaging implements AsyncFunction<ResultSet, ResultSet> {
    private final PageBuilder pageBuilder;
    private final List<ColumnWriter> writers;
    private final Object lock;

    public AsyncPaging(PageBuilder pageBuilder, List<ColumnWriter> writers, Object lock) {
      this.pageBuilder = pageBuilder;
      this.writers = writers;
      this.lock = lock;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public ListenableFuture<ResultSet> apply(ResultSet rs) {
      if (rs == null) {
        return null;
      }

      int remainingsInPage = rs.getAvailableWithoutFetching();
      for (Row row : rs) {
        synchronized (lock) {
          writers.forEach(writer -> writer.write(row, pageBuilder));
          pageBuilder.addRecord();
        }
        if (--remainingsInPage == 0) {
          break;
        }
      }

      boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;

      if (wasLastPage) {
        return Futures.immediateFuture(rs);
      } else {
        ListenableFuture<ResultSet> moreResults = rs.fetchMoreResults();
        return Futures.transformAsync(moreResults, new AsyncPaging(pageBuilder, writers, lock));
      }
    }
  }

  private PreparedStatement buildStatement(Session session,
      List<ColumnMetadata> partitionKeys, PluginTask task) {
    Selection select = QueryBuilder.select();
    if (task.getSelect().isEmpty()) {
      select.all();
    } else {
      task.getSelect().forEach(select::column);
    }
    Where where = select.from(task.getKeyspace(), task.getTable()).where();
    partitionKeys.forEach(
        columnMetadata ->
            where.and(
                QueryBuilder.eq(
                    columnMetadata.getName(), QueryBuilder.bindMarker(columnMetadata.getName()))));

    logger.debug(where.getQueryString());

    return session.prepare(where);
  }

  private Statement bindStatement(PreparedStatement prepared, List<ColumnMetadata> partitionKeys, Row partitionKeysRow) {
    BoundStatement bound = prepared.bind();
    partitionKeys.forEach(
        key -> {
          TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(key.getType());
          bound.set(key.getName(), partitionKeysRow.get(key.getName(), typeCodec), typeCodec);
        });

    return bound;
  }

  List<ColumnWriter> buildWriters(Schema schema, List<ColumnMetadata> columnMetadatas) {
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
                      metadata -> {
                        writersBuilder.add(ColumnWriterFactory.get(index, metadata.getType()));
                      });
            });
    return writersBuilder.build();
  }

  @Override
  public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
    PluginTask task = taskSource.loadTask(PluginTask.class);
    int concurrency = task.getConcurrency();
    final Object lock = new Object();

    Cluster cluster = getCluster(task);
    try (Session session = cluster.newSession()) {
      TableMetadata tableMetadata = getTableMetadata(cluster, task);
      List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
      List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
      List<ColumnWriter> writers = buildWriters(schema, columnMetadatas);

      ResultSet partitionKeyResultSet =
          fetchPartitionKeys(session, task.getKeyspace(), task.getTable(), partitionKeys);

      PreparedStatement prepared = buildStatement(session, partitionKeys, task);

      BufferAllocator allocator = task.getBufferAllocator();
      PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

      for (Row partitionKeysRow : partitionKeyResultSet) {
        if (partitionKeyResultSet.getAvailableWithoutFetching() == 100
            && !partitionKeyResultSet.isFullyFetched()) {
          partitionKeyResultSet.fetchMoreResults();
        }

        long chunkNum = Math.abs((long) partitionKeysRow.getPartitionKeyToken().getValue()) % concurrency;
        if (chunkNum == taskIndex) {
          Statement statement = bindStatement(prepared, partitionKeys, partitionKeysRow);
          ResultSetFuture future = session.executeAsync(statement);
          @SuppressWarnings("UnstableApiUsage")
          ListenableFuture<Object> transform =
              Futures.transformAsync(future, new AsyncPaging(pageBuilder, writers, lock));
          try {
            transform.get();
          } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }

      pageBuilder.finish();
    }

    return Exec.newTaskReport();
  }

  @Override
  public ConfigDiff guess(ConfigSource config) {
    return Exec.newConfigDiff();
  }
}
