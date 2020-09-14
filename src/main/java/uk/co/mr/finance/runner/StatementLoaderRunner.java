package uk.co.mr.finance.runner;

import io.vavr.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import uk.co.mr.finance.domain.Statement;
import uk.co.mr.finance.domain.StatementSummary;
import uk.co.mr.finance.load.DatabaseManager;
import uk.co.mr.finance.load.MultiStatementLoader;
import uk.co.mr.finance.load.StatementPathLoader;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static picocli.CommandLine.Command;

@Command(description = "Loads statement file into database", showDefaultValues = true)
public class StatementLoaderRunner implements Callable<Iterable<Tuple2<Optional<Throwable>, Optional<StatementSummary>>>> {
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
  public Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> call() {
    return loadMultipleFiles(null == toLoadPath ? List.of() : List.of(toLoadPath));
  }

  private Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> loadMultipleFiles(Collection<? extends Path> fileNamesToLoad) {
    Instant is = Instant.now();

    DatabaseManager databaseManager = DatabaseManager.from(driverName, connectString, userId, cleanPassword);

    MultiStatementLoader multiStatementLoader = new MultiStatementLoader(databaseManager);
    Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> loadResults = multiStatementLoader.load(fileNamesToLoad);
    databaseManager.safeClose();

    Duration duration = Duration.between(is, Instant.now());

    printSummaryResults(loadResults, duration);

    return loadResults;
  }

  private void printSummaryResults(Collection<? extends Tuple2<Optional<Throwable>, ? extends Optional<?>>> loadResults, Duration duration) {
    String message = """
        Total Time: [%s]
        Exception Type: [%s]
        Exception Message: [%s]
        Summary: [%s]
        """;

    long loadCommands = loadResults.stream()
                                   .map(tuple -> message.formatted(duration.toString(),
                                                                   tuple._1()
                                                                        .map(t -> t.getClass().getCanonicalName())
                                                                        .orElse("No exception occurred"),
                                                                   tuple._1().map(Throwable::getMessage).orElse("No exception occurred"),
                                                                   tuple._2().map(Object::toString).orElse("No summary was produced")))
                                   .peek(m -> LOG.info("{}", m))
                                   .count();
  }

  private List<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> loadFiles(StatementPathLoader loader, Collection<? extends Path> fileNamesToLoad) {
    return fileNamesToLoad.stream().map(f -> loader.load(f, Statement.transformToStatement())).collect(Collectors.toList());
  }

}
