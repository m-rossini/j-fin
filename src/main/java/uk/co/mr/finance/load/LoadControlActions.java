package uk.co.mr.finance.load;

import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

import static uk.co.mr.finance.db.Tables.LOAD_CONTROL;

public class LoadControlActions {
    private static final Logger LOG = LoggerFactory.getLogger(LoadControlActions.class);

    private final DSLContext ctx;

    public LoadControlActions(DSLContext ctx) {
        this.ctx = ctx;
    }

    public Try<Integer> tryInsertLoadControl(String dataLocation) {
        return Try.of(() -> performInsert(dataLocation))
                  .peek(i -> LOG.info("Inserted RowId:[{}] into load control for data location:[{}]", i, dataLocation))
                  .onFailure(e -> LOG.error("Failed to insert row into load control", e));
    }

    private Integer performInsert(String dataLocation) {
        Record record = ctx.insertInto(LOAD_CONTROL,
                                       LOAD_CONTROL.FILE_NAME,
                                       LOAD_CONTROL.LOAD_DATE,
                                       LOAD_CONTROL.LOAD_IN_PROGRESS)
                           .values(dataLocation, LocalDate.now(), Boolean.TRUE)
                           .returning(LOAD_CONTROL.CONTROL_ID)
                           .fetchOne();

        return record.getValue(LOAD_CONTROL.CONTROL_ID);
    }

    public Try<Integer> tryUpdateLoadControl(Integer controlId, StatementSummary ss) {
        return Try.of(() -> performUpdate(controlId, ss))
                  .onFailure(e -> LOG.warn("Failed to update row into load control", e));
    }

    public int performUpdate(Integer controlId, StatementSummary ss) {
        return ctx.update(LOAD_CONTROL)
                  .set(LOAD_CONTROL.LOAD_IN_PROGRESS, false)
                  .set(LOAD_CONTROL.SMALLEST_DATE, ss.minDate())
                  .set(LOAD_CONTROL.GREATEST_DATE, ss.maxDate())
                  .set(LOAD_CONTROL.LOADED_RECORDS, Long.valueOf(ss.getCount()).intValue())
                  .where(LOAD_CONTROL.CONTROL_ID.eq(controlId))
                  .execute();
    }
}
