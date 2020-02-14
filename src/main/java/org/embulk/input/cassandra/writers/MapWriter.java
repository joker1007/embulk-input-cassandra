package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

@SuppressWarnings({"UnstableApiUsage", "rawtypes", "unchecked"})
public class MapWriter extends CollectionColumnWriter {

  public MapWriter(int columnIndex, DataType cassandraDataType) {
    super(columnIndex, cassandraDataType);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    List<TypeToken> elementTypeTokens = getElementTypeTokens();
    Map<Object, Object> map = row.getMap(getColumnIndex(), elementTypeTokens.get(0), elementTypeTokens.get(1));
    Value value = convertMapToMsgPack(map);
    pageBuilder.setJson(getColumnIndex(), value);
  }
}
