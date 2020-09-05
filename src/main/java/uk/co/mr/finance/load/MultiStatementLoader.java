package uk.co.mr.finance.load;

import io.vavr.Tuple2;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;
import uk.co.mr.finance.domain.StatementSummary;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiStatementLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiStatementLoader.class);

  private DatabaseManager databaseManager;

  public MultiStatementLoader(DatabaseManager databaseManager) {
    this.databaseManager = databaseManager;
  }

  public Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> load(Collection<? extends Path> fileNamesToLoad) {
    try (DSLContext ctx = databaseManager.getConnection()
                                         .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                                         .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"))) {


      StatementLoader loader = new StatementLoader(databaseManager,
                                                   new InputDataManager(),
                                                   new LoadControlActions(ctx),
                                                   new StatementActions(ctx));

      return loadFiles(loader, fileNamesToLoad);
    }
  }

  private List<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> loadFiles(StatementLoader loader, Collection<? extends Path> fileNamesToLoad) {
    return fileNamesToLoad.stream().map(f -> loader.load(f, Statement.transformToStatement())).collect(Collectors.toList());
  }

}
