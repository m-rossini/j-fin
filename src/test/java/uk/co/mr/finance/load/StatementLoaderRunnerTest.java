package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.Tuple2;
import liquibase.exception.LiquibaseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import uk.co.mr.finance.exception.LoaderException;
import uk.co.mr.finance.runner.StatementLoaderRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.co.mr.finance.load.UtilForTest.createDatabase;

public class StatementLoaderRunnerTest {
  private static final Logger LOG = LoggerFactory.getLogger(StatementLoaderRunnerTest.class);

  public static final String USER_ID = "finance";
  public static final String PASS_WORD = "finance";

  public static final String DATABASE_NAME = "finance";
  @Container
  private static final PostgreSQLContainer container =
      new PostgreSQLContainer<>("postgres:latest")
          .withDatabaseName(DATABASE_NAME)
          .withUsername(USER_ID)
          .withPassword(PASS_WORD)
          .withExposedPorts(5432);

  private static Connection connection;
  private static FileSystem fileSystem;

  @BeforeAll
  public static void setup() throws SQLException, LiquibaseException {
    LOG.info("Starting up");
    container.start();
    connection = DriverManager.getConnection(container.getJdbcUrl(), USER_ID, "finance");

    createDatabase(connection);

    fileSystem = Jimfs.newFileSystem(Configuration.windows());
  }

  @Test
  @DisplayName("All parameters are null")
  public void test_runner_null_parameters() {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    Tuple2<Optional<Throwable>, Optional<StatementSummary>> call = runner.call();
    assertNotNull(call);
    assertThat(call._1().isPresent(), is(equalTo(true)));
    assertThat(call._1().get(), instanceOf(NullPointerException.class));
    assertThat(call._2(), is(Optional.empty()));
  }

  @Test
  @DisplayName("Driver Name is provided but not other connection properties")
  public void test_runner_driver_informed() throws Throwable {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    new FieldSetter(runner, runner.getClass().getField("driverName")).set(container.getDriverClassName());

    Tuple2<Optional<Throwable>, Optional<StatementSummary>> call = runner.call();
    assertNotNull(call);
    assertThat(call._1().isPresent(), is(equalTo(true)));
    assertThat(call._1().get(), instanceOf(SQLException.class));
    assertThat(call._1().get().getMessage(), is("The url cannot be null"));
    assertThat(call._2(), is(Optional.empty()));

  }

  @Test
  @DisplayName("Driver Name and URL are provided but not other connection properties")
  public void test_runner_driver_and_url_informed() throws Throwable {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    new FieldSetter(runner, runner.getClass().getField("driverName")).set(container.getDriverClassName());
    new FieldSetter(runner, runner.getClass().getField("connectString")).set(container.getJdbcUrl());

    Tuple2<Optional<Throwable>, Optional<StatementSummary>> call = runner.call();
    assertNotNull(call);
    assertThat(call._1().isPresent(), is(equalTo(true)));
    assertThat(call._1().get(), instanceOf(PSQLException.class));
    assertThat(call._1().get().getMessage(),
               is("The server requested password-based authentication, but no password was provided."));
    assertThat(call._2(), is(Optional.empty()));
  }

  @Test
  @DisplayName("Driver Name, URL and All are provided with wrong file name")
  public void test_runner_driver_and_url_all_informed() throws Throwable {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    new FieldSetter(runner, runner.getClass().getField("driverName")).set(container.getDriverClassName());
    new FieldSetter(runner, runner.getClass().getField("connectString")).set(container.getJdbcUrl());
    new FieldSetter(runner, runner.getClass().getField("userId")).set("finance");
    new FieldSetter(runner, runner.getClass().getField("cleanPassword")).set("finance");

    new FieldSetter(runner, runner.getClass().getDeclaredField("toLoadPath")).set(Paths.get("non-existent-file"));

    Tuple2<Optional<Throwable>, Optional<StatementSummary>> call = runner.call();
    assertThat(call._2(), is(Optional.empty()));

    IOException exception =
        call._1()
            .map(IOException.class::cast)
            .orElseThrow(() -> new IllegalArgumentException("Should have an IOException"));
    assertThat(exception.getMessage(),
               is("File [non-existent-file] cannot be read"));
  }

  @Test
  @DisplayName("All Informed and should run Ok")
  public void test_runner_all_informed() throws Throwable {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    new FieldSetter(runner, runner.getClass().getField("driverName")).set(container.getDriverClassName());
    new FieldSetter(runner, runner.getClass().getField("connectString")).set(container.getJdbcUrl());
    new FieldSetter(runner, runner.getClass().getField("userId")).set("finance");
    new FieldSetter(runner, runner.getClass().getField("cleanPassword")).set("finance");

    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        29/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR,114.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);

    new FieldSetter(runner, runner.getClass().getDeclaredField("toLoadPath")).set(path);

    Tuple2<Optional<Throwable>, Optional<StatementSummary>> results = runner.call();
    assertNotNull(results);
    assertThat(results._2().orElseThrow(() -> new LoaderException("Should have a summary")).getCount(),
               is(1L));

    assertThat(results._2().orElseThrow(() -> new LoaderException("Should have a total amount")).totalAmount(),
               equalTo(new BigDecimal("-114.00")));
  }
}