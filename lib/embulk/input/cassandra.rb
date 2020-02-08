Embulk::JavaPlugin.register_input(
  "cassandra", "org.embulk.input.cassandra.CassandraInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
