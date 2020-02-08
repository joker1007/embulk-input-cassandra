package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;

public class DateWriter extends ColumnWriter {

  public DateWriter(int columnIndex) {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    pageBuilder.setTimestamp(getColumnIndex(), Timestamp.ofEpochMilli(row.getDate(getColumnIndex()).getMillisSinceEpoch()));
  }
}
