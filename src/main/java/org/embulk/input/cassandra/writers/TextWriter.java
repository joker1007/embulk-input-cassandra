package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public class TextWriter extends ColumnWriter
{
  public TextWriter(int columnIndex)
  {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder)
  {
    pageBuilder.setString(getColumnIndex(), row.getString(getColumnIndex()));
  }
}
