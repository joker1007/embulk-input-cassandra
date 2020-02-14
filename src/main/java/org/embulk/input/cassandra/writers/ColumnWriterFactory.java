package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.DataType;

public class ColumnWriterFactory {

  private ColumnWriterFactory() {
  }

  public static ColumnWriter get(int i, DataType cassandraDataType) {
    switch (cassandraDataType.getName()) {
      case INT:
        return new IntWriter(i);
      case BIGINT:
      case COUNTER:
        return new BigintWriter(i);
      case TINYINT:
        return new TinyIntWriter(i);
      case VARINT:
        return new VarIntWriter(i);
      case SMALLINT:
        return new SmallIntWriter(i);
      case TEXT:
      case VARCHAR:
      case ASCII:
        return new TextWriter(i);
      case UUID:
      case TIMEUUID:
        return new UUIDWriter(i);
      case INET:
        return new InetWriter(i);
      case DECIMAL:
        return new DecimalWriter(i);
      case DOUBLE:
        return new DoubleWriter(i);
      case FLOAT:
        return new FloatWriter(i);
      case DATE:
        return new DateWriter(i);
      case TIMESTAMP:
        return new TimestampWriter(i);
      case TIME:
        return new TimeWriter(i);
      case BOOLEAN:
        return new BooleanWriter(i);
      case MAP:
        return new MapWriter(i, cassandraDataType);
      case LIST:
        return new ListWriter(i, cassandraDataType);
      case SET:
        return new SetWriter(i, cassandraDataType);
      default:
        throw new RuntimeException("Unsupported cassandra data type");
    }
  }
}
