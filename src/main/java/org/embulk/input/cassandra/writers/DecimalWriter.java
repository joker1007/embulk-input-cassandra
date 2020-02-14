package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public class DecimalWriter extends ColumnWriter
{
  public DecimalWriter(int columnIndex)
  {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder)
  {
    pageBuilder.setString(getColumnIndex(), row.getDecimal(getColumnIndex()).toString());
  }
}
