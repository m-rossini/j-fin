package uk.co.mr.finance.load;

import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;
import uk.co.mr.finance.domain.StatementSummary;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiStatementLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiStatementLoader.class);
  private DatabaseManager databaseManager;
  private int commitEvery;

  public MultiStatementLoader(DatabaseManager databaseManager) {
    this.databaseManager = databaseManager;
  }

  public Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> load(Collection<? extends Path> fileNamesToLoad) {
    Try<Connection> connection = databaseManager.getConnection();
    try (DSLContext ctx = connection
        .peek(c -> LOG.debug(">>>Connection A:[{}}", connection))
        .map(c -> DSL.using(c, SQLDialect.POSTGRES))
        .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"))) {

      commitEvery = 100;
      return connection.map(c -> new StatementPathLoader(c,
                                                         new InputDataManager(),
                                                         new LoadControlActions(ctx),
                                                         new StatementActions(ctx),
                                                         commitEvery)
                            )
                       .map(loader -> loadFiles(loader, fileNamesToLoad))
                       .andFinally(() -> connection.peek(DatabaseManager::safeCloseConnection))
                       .getOrElseThrow(() -> new RuntimeException("Should have a tuple as result from load"));
    }
  }

  private List<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> loadFiles(StatementPathLoader loader, Collection<? extends Path> fileNamesToLoad) {
    return fileNamesToLoad.stream().map(f -> loader.load(f, Statement.transformToStatement())).collect(Collectors.toList());
  }

}
