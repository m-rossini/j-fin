package uk.co.mr.finance.load;

import io.vavr.control.Try;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.StatementSummary;
import uk.co.mr.finance.exception.LoaderException;

import java.time.LocalDate;
import java.util.Optional;

import static uk.co.mr.finance.db.Tables.LOAD_CONTROL;

public class LoadControlActions {
  private static final Logger LOG = LoggerFactory.getLogger(LoadControlActions.class);

  private final DSLContext ctx;

  public LoadControlActions(DSLContext ctx) {
    this.ctx = ctx;
  }

  public Try<Integer> tryInsertLoadControl(String dataLocation, String hash) {
    return Try.of(() -> performInsert(dataLocation, hash))
              .peek(i -> LOG.info("Inserted RowId:[{}] into load control for data location:[{}]", i, dataLocation))
              .onFailure(e -> LOG.error("Failed to insert row into load control", e));
  }

  private Integer performInsert(String dataLocation, String hash) {
    Record record = ctx.insertInto(LOAD_CONTROL,
                                   LOAD_CONTROL.FILE_NAME,
                                   LOAD_CONTROL.LOAD_DATE,
                                   LOAD_CONTROL.LOAD_IN_PROGRESS,
                                   LOAD_CONTROL.FILE_HASH_CODE)
                       .values(dataLocation, LocalDate.now(), Boolean.TRUE, hash)
                       .returning(LOAD_CONTROL.CONTROL_ID)
                       .fetchOne();

    return record.getValue(LOAD_CONTROL.CONTROL_ID);
  }

  public Try<Integer> tryCloseLoadControl(int controlId) {
    return Try.of(() -> performClose(controlId))
              .peek(i -> LOG.info("Closing load control for Id:[{}]", i))
              .onFailure(e -> LOG.error("Failed to close load control for id:[{}]", controlId, e));
  }

  private int performClose(int controlId) {
    ctx.update(LOAD_CONTROL)
       .set(LOAD_CONTROL.LOAD_IN_PROGRESS, false)
       .execute();
    return controlId;
  }

  public Try<String> tryCanInsert(String hash) {
    return Try.of(() -> canInsert(hash))
              .flatMap(o -> o.map(fileName -> Try.<String>failure(new LoaderException(String.format("hashCode [%s] already loaded in file:[%s]",
                                                                                                    hash,
                                                                                                    fileName))))
                             .orElse(Try.success(hash)));

  }

  private Optional<String> canInsert(String hash) {
    return ctx.selectDistinct(LOAD_CONTROL.FILE_NAME)
              .from(LOAD_CONTROL)
              .where(LOAD_CONTROL.FILE_HASH_CODE.eq(hash))
              .fetchOptional()
              .map(r -> r.get(LOAD_CONTROL.FILE_NAME));
  }

  public Try<Integer> tryUpdateLoadControl(Integer controlId, StatementSummary ss) {
    return Try.of(() -> performUpdate(controlId, ss))
              .onFailure(e -> LOG.warn("Failed to update row into load control", e));
  }

  public int performUpdate(Integer controlId, StatementSummary ss) {
    ctx.update(LOAD_CONTROL)
       .set(LOAD_CONTROL.SMALLEST_DATE, ss.minDate())
       .set(LOAD_CONTROL.GREATEST_DATE, ss.maxDate())
       .set(LOAD_CONTROL.LOADED_RECORDS, Long.valueOf(ss.getCount()).intValue())
       .where(LOAD_CONTROL.CONTROL_ID.eq(controlId))
       .execute();
    return controlId;
  }
}
