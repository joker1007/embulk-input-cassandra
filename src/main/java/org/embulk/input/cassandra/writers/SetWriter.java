package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

import java.util.Set;

@SuppressWarnings({"UnstableApiUsage", "unchecked"})
public class SetWriter extends CollectionColumnWriter
{
  public SetWriter(int columnIndex, DataType cassandraDataType)
  {
    super(columnIndex, cassandraDataType);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder)
  {
    Set<Object> set = row.getSet(getColumnIndex(), getElementTypeTokens().get(0));
    Value value = convertSetToMsgPack(set);
    pageBuilder.setJson(getColumnIndex(), value);
  }
}
