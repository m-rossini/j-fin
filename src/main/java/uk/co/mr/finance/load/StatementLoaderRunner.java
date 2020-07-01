package uk.co.mr.finance.load;

import io.vavr.control.Either;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static picocli.CommandLine.Command;

@Command(description = "Loads statement file into database", showDefaultValues = true)
public class StatementLoaderRunner implements Callable<StatementSummary> {
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
    public StatementSummary call() {
        Instant is = Instant.now();

        DatabaseManager databaseManager = new DatabaseManager(driverName, connectString, userId, cleanPassword);
        Try<Connection> connection = databaseManager.getConnection();

        Either<Throwable, StatementSummary> summaries =
                connection.map(c -> new LoadGuide(databaseManager))
                          .toEither()
                          .flatMap(loadGuide -> loadGuide.guide(toLoadPath));

        connection.peek(c -> databaseManager.safeClose());

        Duration duration = Duration.between(is, Instant.now());

        return summaries.peek(s -> LOG.info("Total Time:[{}]\nValue returned from load:[{}]", duration, s))
                        .getOrElseThrow(l -> {
                            throw new IllegalArgumentException(l);
                        });
    }

}
