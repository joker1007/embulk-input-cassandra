package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public class TinyIntWriter extends ColumnWriter {

  public TinyIntWriter(int columnIndex) {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    pageBuilder.setLong(getColumnIndex(), row.getByte(getColumnIndex()));
  }
}
