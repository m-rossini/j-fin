package uk.co.mr.finance.load;

import io.vavr.Tuple2;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.control.Option;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.nio.file.Path;
import java.sql.Savepoint;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.filtering;
import static java.util.stream.Collectors.flatMapping;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;
import static java.util.stream.Collectors.teeing;
import static java.util.stream.Collectors.toList;
import static uk.co.mr.finance.db.Tables.LOAD_CONTROL;
import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

public class StatementLoader implements DataLoader<Statement, DoubleSummaryStatistics> {
  private static final Logger LOG = LoggerFactory.getLogger(StatementLoader.class);

  private final DSLContext ctx;
  private final LoadControlActions loadControlActions;
  private Savepoint savePoint;
  private final FileManager fileManager;
  private final DatabaseManager dbManager;
  public static final String ERR_MSG_REC_PRO = "This exception occurred while processing statements records";
  public static final String ERR_MSG_REC_GEN = "This exception occurred while generating statements records";

  //TODO Replace JOOQ by R2DBC
  public StatementLoader(DatabaseManager dbManager, FileManager fileManager) {
    this.dbManager = dbManager;
    dbManager.safeSetAutoCommitOff();

    this.fileManager = fileManager;

    ctx = dbManager.getConnection()
                   .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                   .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"));

    loadControlActions = new LoadControlActions(this.ctx);
  }

  private Try<Integer> openLoadControl(Path path) {
    return fileManager.mayHashFile(path)
                      .onFailure(t -> LOG.error("Error trying to read file", t))
                      .flatMap(this::tryCanInsert)
                      .flatMap(hash -> loadControlActions.tryInsertLoadControl(path.toAbsolutePath().toString(), hash))
                      .peek(id -> dbManager.safeCommit());
  }

  private void printError(String errMsg, List<Throwable> errorList) {
    errorList.forEach(t -> LOG.error(errMsg, t));
  }

  private Tuple2<List<Throwable>, List<Statement>> collectStatements(Stream<? extends Validation<Seq<Throwable>, Statement>> v) {
    return v.collect(teeing(filtering(Validation::isInvalid,
                                      flatMapping(t -> t.getError().toJavaList().stream(), toList())),
                            filtering(Validation::isValid,
                                      mapping(Validation::get, toList())),
                            Tuple2::new));
  }

  private Tuple2<List<Throwable>, Optional<StatementSummary>> collectResults(Stream<? extends Try<Statement>> statementsStream) {
    return statementsStream.collect(
        teeing(
            filtering(Try::isFailure, mapping(Try::getCause, toList())),
            filtering(Try::isSuccess, mapping(Try::get,
                                              mapping(StatementSummary::fromStatement,
                                                      reducing(StatementSummary::merge)))),
            Tuple2::new));
  }

  @Override
  public Tuple2<Optional<Throwable>, Optional<StatementSummary>>
  load(Path path, Function<String[], Validation<Seq<Throwable>, Statement>> transformer) {

    Try<Integer> tryOpenControl = openLoadControl(path).peek(controlId -> dbManager.safeCommit());

    AtomicReference<Throwable> hasIntermediateErrors =
        tryOpenControl.transform(f -> f.isFailure() ? new AtomicReference<>(f.getCause()) : new AtomicReference<>());

    Try<StatementSummary> results =
        tryOpenControl.flatMap(controlId -> fileManager.transformFile(path, transformer))
                      .map(this::collectStatements)
                      .peek(u -> handleThrowable(u._1(), hasIntermediateErrors, ERR_MSG_REC_GEN))
                      .map(Tuple2::_2)
                      .map(Collection::stream)
                      .map(this::processStatements)
                      .map(Collection::stream)
                      .map(this::collectResults)
                      .peek(u -> handleThrowable(u._1(), hasIntermediateErrors, ERR_MSG_REC_PRO))
                      .map(Tuple2::_2)
                      .map(Option::ofOptional)
                      .peek(o -> o.peek(ss -> tryReorderData()))
                      .peek(o -> o.peek(s -> dbManager.safeCommit()))
                      .map(o -> o.getOrElse(StatementSummary.DEFAULT_STATEMENT));

    Try<Integer> updateAndClose =
        tryOpenControl.peek(controlId -> results.peek(ss -> loadControlActions.tryUpdateLoadControl(controlId, ss)))
                      .andThenTry(loadControlActions::tryCloseLoadControl)
                      .onFailure(t -> dbManager.safeRollback())
                      .onSuccess(i -> dbManager.safeCommit());

    Optional<Throwable> part1 =
        Stream.concat(List.of(tryOpenControl, results, updateAndClose).stream()
                          .filter(Try::isFailure)
                          .map(Try::getCause),
                      Stream.of(hasIntermediateErrors.get()))
              .filter(Objects::nonNull)
              .distinct()
              .sorted()
              .reduce((t1, t2) -> t1);


    return new Tuple2<>(part1,
                        results.toJavaOptional().filter(ss -> ss != StatementSummary.DEFAULT_STATEMENT));
  }

  private void handleThrowable(List<Throwable> errorList,
                               AtomicReference<? super Throwable> hasIntermediateErrors,
                               String message) {
    printError(message, errorList);
    if (!errorList.isEmpty()) {
      hasIntermediateErrors.set(errorList.get(0));
    }
  }


  private List<Try<Statement>> processStatements(Stream<Statement> statements) {
    LOG.info("About to to insert into statements table");
    return statements.peek(s -> LOG.info("Processing Statement:[{}]", s))
                     .map(this::safeActionFunction)
                     .peek(tryOf -> LOG.info("Result of insertion of statement:[{}]", tryOf))
                     .map(this::savePointOrRollback)
                     .collect(toList());
  }

  //TODO move can insert methods to load control
  private Try<String> tryCanInsert(String hash) {
    return Try.of(() -> canInsert(hash))
              .filter(b -> b)
              .map(b -> hash);
  }

  private boolean canInsert(String hash) {
    return ctx.selectDistinct(LOAD_CONTROL.FILE_HASH_CODE)
              .from(LOAD_CONTROL)
              .where(LOAD_CONTROL.FILE_HASH_CODE.eq(hash))
              .fetchOptional()
              .map(r -> r.get(LOAD_CONTROL.FILE_HASH_CODE))
              .isEmpty();
  }

  private Try<Statement> safeActionFunction(Statement statement) {
    return Try.of(() -> insertIntoStatement(statement))
              .onFailure(t -> LOG.error("Failed to insert into statement table"));
  }

  private Try<Statement> savePointOrRollback(Try<Statement> tryStatement) {
    Consumer<Throwable> errorMessage =
        t -> LOG.warn("Insert did not succeeded, rolling back transaction with one 1 statement", t.getCause());

    return tryStatement.onFailure(errorMessage)
                       .onFailure(t -> {
                         if (null == this.savePoint) {
                           LOG.info("Save point is null, rolling back everything");
                           dbManager.safeRollback();
                         } else {
                           LOG.info("Rolling back to save point:[{}]", savePoint);
                           dbManager.safeRollbackTo(this.savePoint);
                         }
                       })
                       .peek(statement -> {
                         this.savePoint = dbManager.safeSetSavePoint().getOrElse((Savepoint) null);
                         LOG.info("Save point set to:[{}]", savePoint);
                       });
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

  private int tryReorderData() {
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


}