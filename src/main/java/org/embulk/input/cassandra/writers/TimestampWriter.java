package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.Row;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;

public class TimestampWriter extends ColumnWriter {

  public TimestampWriter(int columnIndex) {
    super(columnIndex);
  }

  @Override
  protected void doWrite(Row row, PageBuilder pageBuilder) {
    pageBuilder.setTimestamp(getColumnIndex(), Timestamp.ofEpochMilli(row.getTimestamp(getColumnIndex()).getTime()));
  }
}
