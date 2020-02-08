package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public class FloatWriter extends ColumnWriter {

  public FloatWriter(int columnIndex) {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    pageBuilder.setDouble(getColumnIndex(), row.getFloat(getColumnIndex()));
  }
}
