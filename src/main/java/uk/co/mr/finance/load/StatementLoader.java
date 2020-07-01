package uk.co.mr.finance.load;

import io.vavr.collection.Iterator;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.nio.file.Path;
import java.sql.Savepoint;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

public class StatementLoader implements DataLoader<Statement, DoubleSummaryStatistics> {
    private static final Logger LOG = LoggerFactory.getLogger(StatementLoader.class);

    private final DSLContext ctx;
    private final LoadControlActions loadControlActions;
    private Savepoint savePoint;
    private FileManager fileManager;
    private DatabaseManager dbManager;

    public StatementLoader(DatabaseManager dbManager, FileManager fileManager) {
        this.dbManager = dbManager;
        dbManager.safeSetAutoCommitOff();

        this.fileManager = fileManager;

        ctx = dbManager.getConnection()
                       .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                       .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"));

        loadControlActions = new LoadControlActions(this.ctx);
    }

    private Either<Throwable, Integer> insertLoadControl(Path path) {
        return fileManager.mayHashFile(path)
                          .onFailure(t -> LOG.error("Error trying to read file", t))
                          .flatMap(this::tryCanInsert)
                          .flatMap(b -> loadControlActions.tryInsertLoadControl(path.toAbsolutePath().toString()))
                          .peek(id -> dbManager.safeCommit())
                          .toEither();
    }

    @Override
    public Either<Throwable, StatementSummary> load(Path path,
                                                    Function<? super String[], Optional<Statement>> transformer) {

        Either<Throwable, Integer> maybeControlId = insertLoadControl(path);
        if (maybeControlId.isLeft()) {
            return Either.left(maybeControlId.getLeft());
        }

        Try<Stream<Statement>> statements = fileManager.transformFile(path, transformer);
        if (statements.isFailure()) {
            return Either.left(statements.getCause());
        }

        Try<StatementSummary> trySummary =
                statements.map(this::processStatements)
                          .andThen(l -> LOG.info("{} rows inserted and waiting to be committed", l.size()))
                          .andFinally(() -> dbManager.safeCommit())
                          .andFinally(this::reorderData)
                          .flatMapTry(this::calculateSummary);

        Try<Integer> tryUpdate =
                trySummary.flatMap(s -> loadControlActions.tryUpdateLoadControl(maybeControlId.get(), s));
        if (tryUpdate.isFailure()) {
            return Either.left(tryUpdate.getCause());
        }

        return trySummary.toEither();
    }

    private Try<StatementSummary> calculateSummary(List<Statement> s) {
        Optional<StatementSummary> reduce = s.stream()
                                             .map(StatementSummary::fromStatement)
                                             .reduce(StatementSummary::merge);
        return Option.ofOptional(reduce).toTry();
    }

    private void reorderData() {
        int reorderedRows = tryReorderData();
        if (reorderedRows > 0) {
            dbManager.safeCommit();
        }
        LOG.info("{} Rows reordered", reorderedRows);
    }

    private List<Statement> processStatements(Stream<Statement> statements) {
        return statements.map(statement -> safeActionFunction(statement))
                         .map(statements1 -> statements1.toEither())
                         .map(either -> savePointOrRollback(either))
                         .flatMap(Optional::stream)
                         .collect(Collectors.toList());
    }

    //TODO move can insert methods to load control
    private Try<String> tryCanInsert(String hash) {
        return Try.of(() -> canInsert(hash))
                  .map(b -> hash);

    }

    private Boolean canInsert(String hash) {
        //TODO Implement this
        return true;
    }

    private Try<Statement> safeActionFunction(Statement statement) {
        return Try.of(() -> insertIntoStatement(statement))
                  .onFailure(t -> LOG.error("Failed to insert into statement table"));
    }

    private Optional<Statement> savePointOrRollback(Either<? extends Throwable, Statement> either) {
        return either.peekLeft(t -> LOG.warn("Insert did not succeeded, rolling back transaction with one 1 statement",
                                             t.getCause()))
                     .peekLeft(t -> {
                         if (null == this.savePoint) {
                             LOG.info("Save point is null, rolling back everything");
                             dbManager.safeRollback();
                         } else {
                             LOG.info("Rolling back to save point:[{}]", savePoint);
                             dbManager.safeRollbackTo(this.savePoint);
                         }
                     })
                     .peek(statement -> {
                         LOG.info("Setting save point");
                         this.savePoint = dbManager.safeSetSavePoint().getOrElse((Savepoint) null);
                     })
                     .toJavaOptional();
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