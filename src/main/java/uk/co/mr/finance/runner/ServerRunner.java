package uk.co.mr.finance.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.javalin.Javalin;
import io.javalin.core.util.RouteOverviewPlugin;
import io.javalin.http.UploadedFile;
import io.javalin.http.staticfiles.Location;
import io.javalin.plugin.json.JavalinJackson;
import io.vavr.Tuple2;
import io.vavr.Value;
import io.vavr.control.Try;
import io.vavr.jackson.datatype.VavrModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.co.mr.finance.domain.StatementSummary;
import uk.co.mr.finance.load.DatabaseManager;
import uk.co.mr.finance.load.MultiStatementLoader;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.path;
import static io.javalin.apibuilder.ApiBuilder.post;

@CommandLine.Command(description = "Loads statement file into database", showDefaultValues = true)
public class ServerRunner implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(ServerRunner.class);

  @CommandLine.Option(
      names = {"-d", "--db-driver"},
      required = true, description = "Database driver name to be used",
      defaultValue = "org.postgresql.Driver")

  public String driverName;

  @CommandLine.Option(
      names = {"-c", "--connect-string"},
      required = true,
      description = "Connection String to be used",
      defaultValue = "jdbc:postgresql://localhost:5432/finance")
  public String connectString;

  @CommandLine.Option(
      names = {"-u", "--user-id"},
      required = false,
      description = "User id to be used. It is optional, depends on database")
  public String userId;

  @CommandLine.Option(names = {"-w", "--password-clean"}, required = false, description = "Clean text password")
  public String cleanPassword;


  //TODO Mutually exclusive encrypted vs non encrypted password
  @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Displays usage help")
  private boolean usageHelpRequested;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    ServerRunner serverRunner = new ServerRunner();
    CommandLine commandLine = new CommandLine(serverRunner);
    CommandLine.ParseResult result = commandLine.parseArgs(args);
    serverRunner.call();
  }

  //TODO Open APi
  //TODO Port as CLI parameter
  //TODO Health Check
  @Override
  public Integer call() {
    DatabaseManager databaseManager = DatabaseManager.from(driverName, connectString, userId, cleanPassword);

    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new VavrModule());
    JavalinJackson.configure(mapper);

    Javalin app =
        Javalin.create(config -> {
          config.registerPlugin(new RouteOverviewPlugin("/help"));
          config.addStaticFiles("/public", Location.CLASSPATH);

        }).start(7000);

    Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

    app.events(event -> {
      event.serverStopping(() -> {
        LOG.info("Server is Stopping");
        databaseManager.safeClose();
      });
      event.serverStopped(() -> LOG.info("Server is Stopped"));
    });

    app.error(404, ctx -> ctx.redirect("/help"));

    app.routes(() -> {
      path("/", () -> {
        post("upload", ctx -> {
          List<UploadedFile> files = ctx.uploadedFiles("files");

          Predicate<UploadedFile> csvFilter = f -> f.getExtension().equals(".csv");
          Predicate<UploadedFile> txtFilter = f -> f.getExtension().equals(".txt");
          List<Path> paths = files.stream()
                                  .filter(csvFilter.or(txtFilter))
                                  .peek(f -> LOG.info("File to be loaded:{}", f))
                                  .map(f -> copyFrom(f, () -> f.getFilename() + "_output_" + UUID.randomUUID()))
                                  .filter(Try::isSuccess)
                                  .map(Value::getOrNull)
                                  .filter(Objects::nonNull)
                                  .collect(Collectors.toList());
          MultiStatementLoader loader = new MultiStatementLoader(databaseManager);
          Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> load = loader.load(paths);
          ctx.json(load);
        });
        get("", ctx -> {
        });
      });
    });

    return 0;
  }

  private Try<Path> copyFrom(UploadedFile input, Supplier<String> fileNameCreator) {
    Path path = Paths.get(fileNameCreator.get());
    path.toFile().deleteOnExit();
    return Try.of(() -> path)
              .mapTry(Files::newOutputStream)
              .andThenTry(o -> input.getContent().transferTo(o))
              .andThenTry(OutputStream::close)
              .map(o -> path)
              .onFailure(t -> LOG.error("Failed to copy file", t));
  }
}
