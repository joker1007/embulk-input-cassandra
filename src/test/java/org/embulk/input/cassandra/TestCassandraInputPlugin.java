package org.embulk.input.cassandra;

import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeTokens;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.IntStream;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.test.TestingEmbulk.RunResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestCassandraInputPlugin
{
  private static final String RESOURCE_PATH = "org/embulk/input/cassandra/";

  @Rule
  public TestingEmbulk embulk = TestingEmbulk.builder()
      .registerPlugin(InputPlugin.class, "cassandra", CassandraInputPlugin.class)
      .build();

  private Cluster cluster;
  private Session session;

  private static String getCassandraHost()
  {
    String host = System.getenv("CASSANDRA_HOST");
    if (host == null) {
      host = "localhost";
    }
    return host;
  }

  private static List<String> getCassandraHostAsList()
  {
    return Collections.singletonList(getCassandraHost());
  }

  private static String getCassandraPort()
  {
    String port = System.getenv("CASSANDRA_PORT");
    if (port == null) {
      port = "9042";
    }
    return port;
  }

  private static Cluster getCluster()
  {
    return Cluster.builder().addContactPoint(getCassandraHost())
        .withPort(Integer.parseInt(getCassandraPort())).build();
  }

  @Before
  public void setup()
  {
    cluster = getCluster();
    session = cluster.connect();
    String createKeyspace = EmbulkTests.readResource(RESOURCE_PATH + "create_keyspace.cql");
    String createTableBasic = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_basic.cql");
    String createTableUuid = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_uuid.cql");
    String createTableComplex = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_complex.cql");
    String createTableCounter = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_counter.cql");
    session.execute(createKeyspace);
    session.execute(createTableBasic);
    session.execute(createTableUuid);
    session.execute(createTableComplex);
    session.execute(createTableCounter);
    session.execute("TRUNCATE embulk_test.test_basic");
    session.execute("TRUNCATE embulk_test.test_uuid");
    session.execute("TRUNCATE embulk_test.test_complex");
    session.execute("TRUNCATE embulk_test.test_counter");
  }

  private void setupBasic() {
    IntStream.range(1, 80000).forEach(i -> {
      Boolean bool = i % 2 == 0 ? true : null;
      Insert insert1 = QueryBuilder.insertInto("embulk_test", "test_basic")
          .value("id", "id-" + i)
          .value("int_item", i)
          .value("int32_item", 2)
          .value("smallint_item", 3)
          .value("tinyint_item", 0)
          .value("boolean_item", bool)
          .value("double_item", 1.0d)
          .value("timestamp_item", Timestamp.from(Instant.now()));

      session.execute(insert1);
    });
  }

  private void setupComplex() {
    IntStream.range(1, 100).forEach(i -> {
      List<String> list = new ArrayList<>();
      list.add(String.valueOf(i));
      list.add(String.valueOf(i + 1));

      Set<Long> set = new HashSet<>();
      set.add((long) i);
      set.add((long) i + 3);

      Map<String, Long> map = new HashMap<>();
      map.put("key-1", (long) i);
      map.put("key-2", (long) i + 10);

      Insert insert1 = QueryBuilder.insertInto("embulk_test", "test_complex")
          .value("id", UUID.randomUUID())
          .value("decimal_item", BigDecimal.valueOf(i))
          .value("date_item", LocalDate.fromMillisSinceEpoch(Instant.now().toEpochMilli()))
          .value("time_item", i)
          .value("list_item", list)
          .value("set_item", set)
          .value("map_item", map);

      session.execute(insert1);
    });
  }

  @After
  public void teardown()
  {
    session.close();
    cluster.close();
  }

  private ConfigSource loadYamlResource(String filename)
  {
    return embulk.loadYamlResource(RESOURCE_PATH + filename);
  }

  @Test
  public void testBasic() throws IOException {
    setupBasic();

    ConfigSource config = loadYamlResource("test_basic.yaml");
    Path outputPath = Paths.get("tmp", "basic_output.csv");
    RunResult result = embulk.runInput(config, outputPath);
    Schema schema = result.getInputSchema();

    List<ColumnMetadata> columnMetadatas = cluster.getMetadata().getKeyspace("embulk_test").getTable("test_basic").getColumns();
    assertEquals(8, columnMetadatas.size());
    for (int i = 0; i < columnMetadatas.size(); i++) {
      assertEquals(columnMetadatas.get(i).getName(), schema.getColumnName(i));
    }
  }

  @Test
  public void testComplex() throws IOException {
    setupComplex();

    ConfigSource config = loadYamlResource("test_complex.yaml");
    Path outputPath = Paths.get("tmp", "complex_output.csv");
    RunResult result = embulk.runInput(config, outputPath);
    Schema schema = result.getInputSchema();

    List<ColumnMetadata> columnMetadatas = cluster.getMetadata().getKeyspace("embulk_test").getTable("test_complex").getColumns();
    assertEquals(8, columnMetadatas.size());
    for (int i = 0; i < columnMetadatas.size(); i++) {
      assertEquals(columnMetadatas.get(i).getName(), schema.getColumnName(i));
    }
  }
}
