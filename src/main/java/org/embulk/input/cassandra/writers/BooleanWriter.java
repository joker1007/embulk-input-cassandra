package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public class BooleanWriter extends ColumnWriter
{
  public BooleanWriter(int columnIndex)
  {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder)
  {
    pageBuilder.setBoolean(getColumnIndex(), row.getBool(getColumnIndex()));
  }
}
