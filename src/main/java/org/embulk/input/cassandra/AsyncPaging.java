package org.embulk.input.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.embulk.input.cassandra.writers.ColumnWriter;
import org.embulk.spi.PageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AsyncPaging implements AsyncFunction<ResultSet, ResultSet>
{
  private final PageBuilder pageBuilder;
  private final List<ColumnWriter> writers;
  private final Object lock;
  private long counter;
  private long loggingThreshold;

  private static Logger logger = LoggerFactory.getLogger(AsyncPaging.class);

  public AsyncPaging(PageBuilder pageBuilder, List<ColumnWriter> writers, Object lock)
  {
    this.pageBuilder = pageBuilder;
    this.writers = writers;
    this.lock = lock;
    this.counter = 0L;
    this.loggingThreshold = 1L;
  }

  private AsyncPaging(PageBuilder pageBuilder, List<ColumnWriter> writers, Object lock, long counter, long loggingThreshold)
  {
    this.pageBuilder = pageBuilder;
    this.writers = writers;
    this.lock = lock;
    this.counter = counter;
    this.loggingThreshold = loggingThreshold;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public ListenableFuture<ResultSet> apply(ResultSet rs)
  {
    if (rs == null) {
      return Futures.immediateFuture(null);
    }

    int remainingsInPage = rs.getAvailableWithoutFetching();
    for (Row row : rs) {
      synchronized (lock) {
        writers.forEach(writer -> writer.write(row, pageBuilder));
        pageBuilder.addRecord();
        counter++;
        if (counter >= loggingThreshold) {
          logger.info("Loaded {} records", counter);
          loggingThreshold = loggingThreshold * 2;
        }
      }
      if (--remainingsInPage == 0) {
        break;
      }
    }

    boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;

    if (wasLastPage) {
      return Futures.immediateFuture(rs);
    }
    else {
      ListenableFuture<ResultSet> moreResults = rs.fetchMoreResults();
      return Futures.transform(moreResults, new AsyncPaging(pageBuilder, writers, lock, counter,
          loggingThreshold));
    }
  }
}
