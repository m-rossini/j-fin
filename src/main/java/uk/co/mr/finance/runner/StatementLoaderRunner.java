package uk.co.mr.finance.runner;

import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import uk.co.mr.finance.domain.Statement;
import uk.co.mr.finance.load.DatabaseManager;
import uk.co.mr.finance.load.FileManager;
import uk.co.mr.finance.load.LoadControlActions;
import uk.co.mr.finance.load.StatementLoader;
import uk.co.mr.finance.load.StatementSummary;
import uk.co.mr.finance.load.StatementlActions;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;

@Command(description = "Loads statement file into database", showDefaultValues = true)
public class StatementLoaderRunner implements Callable<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> {
  private static final Logger LOG = LoggerFactory.getLogger(StatementLoaderRunner.class);

  @Option(names = {"-f", "--file"}, description = "File to load", required = true)
  private Path toLoadPath;

  @Option(names = {"-d", "--db-driver"}, required = true, description = "Database driver name to be used")
  public String driverName;

  @Option(names = {"-c", "--connect-string"}, required = true, description = "Connection String to be used")
  public String connectString;

  @Option(names = {"-u",
                   "--user-id"}, required = false, description = "User id to be used. It is optional, depends on database")
  public String userId;

  @Option(names = {"-w", "--password-clean"}, required = false, description = "Clean text password")
  public String cleanPassword;


  //TODO Mutually exclusive encrypted vs non encrypted password
  @Option(names = {"-h", "--help"}, usageHelp = true, description = "Displays usage help")
  private boolean usageHelpRequested;


  public static void main(String[] args) {
    System.exit(new CommandLine(new StatementLoaderRunner()).execute(args));
  }

  @Override
  public Tuple2<Optional<Throwable>, Optional<StatementSummary>> call() {
    Instant is = Instant.now();

    DatabaseManager databaseManager = new DatabaseManager(driverName, connectString, userId, cleanPassword);
    Try<Connection> connection = databaseManager.getConnection();
    if (connection.isFailure()) {
      return new Tuple2<>(Optional.of(connection.getCause()),
                          Optional.empty());
    }


    try (DSLContext ctx = databaseManager.getConnection()
                                         .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                                         .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"))) {

      Try<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> summaries =
          connection.map(c -> new StatementLoader(databaseManager,
                                                  new FileManager(),
                                                  new LoadControlActions(ctx),
                                                  new StatementlActions(ctx)))
                    .map(loader -> loader.load(toLoadPath, Statement.transformToStatement()));

      connection.peek(c -> databaseManager.safeClose());

      Duration duration = Duration.between(is, Instant.now());

      String message = """
          Total Time: [%s]
          Exception Type: [%s]
          Exception Message: [%s]
          Summary: [%s]
          """
          .formatted(duration.toString(),
                     summaries.get()._1().map(t -> t.getClass().getCanonicalName()).orElse("No exception occurred"),
                     summaries.get()._1().map(Throwable::getMessage).orElse("No exception occurred"),
                     summaries.get()._2().map(Object::toString).orElse("No summary was produced"));
      summaries.peek(s -> LOG.info("Total Time:[{}]\nValue returned from load:[{}]", duration, s));

      return summaries.peek(s -> LOG.info(message))
                      .getOrElseThrow(l -> {
                        throw new IllegalArgumentException(l);
                      });
    }
  }

}
