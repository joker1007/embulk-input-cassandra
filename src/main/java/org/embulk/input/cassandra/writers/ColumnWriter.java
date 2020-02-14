package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;

public abstract class ColumnWriter
{
  private int columnIndex;

  public ColumnWriter(int columnIndex)
  {
    this.columnIndex = columnIndex;
  }

  public void write(Row row, PageBuilder pageBuilder)
  {
    if (row.isNull(columnIndex)) {
      pageBuilder.setNull(columnIndex);
    }
    else {
      doWrite(row, pageBuilder);
    }
  }

  protected abstract void doWrite(Row row, PageBuilder pageBuilder);

  public int getColumnIndex()
  {
    return columnIndex;
  }
}
