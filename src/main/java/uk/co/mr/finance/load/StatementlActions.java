package uk.co.mr.finance.load;

import io.vavr.collection.Iterator;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

public class StatementlActions {
  private static final Logger LOG = LoggerFactory.getLogger(StatementlActions.class);

  private final DSLContext ctx;

  public StatementlActions(DSLContext ctx) {
    this.ctx = ctx;
  }

  public int tryReorderData() {
    //TODO Use windowing functions
    return Try.of(() -> ctx.selectFrom(STATEMENT_DATA)
                           .orderBy(STATEMENT_DATA.STATEMENT_DATE,
                                    STATEMENT_DATA.STATEMENT_ID.desc())
                           .fetch())
              .onFailure(t -> LOG.warn("Failed to select from STATEMENT_DATA", t))
              .map(Iterator::ofAll)
              .getOrElse(Iterator::empty)
              .map(r -> r.getValue(STATEMENT_DATA.STATEMENT_ID))
              .zipWithIndex()
              .map(pair -> updateStatementOrder(pair._1(), pair._2()))
              .count(integer -> true);
  }

  private int updateStatementOrder(Integer transactionId, Integer order) {
    return ctx.update(STATEMENT_DATA)
              .set(STATEMENT_DATA.TRANSACTION_ORDER, order + 1)
              .where(STATEMENT_DATA.STATEMENT_ID.eq(transactionId))
              .execute();
  }

  public Try<Statement> tryInsertIntoStatement(Statement statement) {
    return Try.of(() -> insertIntoStatement(statement))
              .onFailure(t -> LOG.error("Failed to insert into statement table"));
  }

  private Statement insertIntoStatement(Statement statement) {
    Integer statementId =
        ctx.insertInto(STATEMENT_DATA)
           .set(STATEMENT_DATA.TRANSACTION_ORDER, (Integer) null)
           .set(STATEMENT_DATA.STATEMENT_DATE, statement.transactionDate())
           .set(STATEMENT_DATA.TRANSACTION_TYPE, statement.transactionTypeCode())
           .set(STATEMENT_DATA.SORT_CODE, statement.sortCode())
           .set(STATEMENT_DATA.ACCOUNT_ID, statement.accountId())
           .set(STATEMENT_DATA.TRANSACTION_DESCRIPTION, statement.transactionDescription())
           .set(STATEMENT_DATA.TRANSACTION_AMOUNT, statement.transactionAmount())
           .set(STATEMENT_DATA.TOTAL_BALANCE, statement.totalBalance())
           .returningResult(STATEMENT_DATA.STATEMENT_ID)
           .fetchOne()
           .into(Integer.class);

    return statement.withStatementId(statementId);
  }

}
