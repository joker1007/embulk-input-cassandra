package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import java.util.List;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

@SuppressWarnings({"UnstableApiUsage", "unchecked"})
public class ListWriter extends CollectionColumnWriter {

  public ListWriter(int columnIndex, DataType cassandraDataType) {
    super(columnIndex, cassandraDataType);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    List<Object> list = row.getList(getColumnIndex(), getElementTypeTokens().get(0));
    Value value = convertListToMsgPack(list);
    pageBuilder.setJson(getColumnIndex(), value);
  }
}
