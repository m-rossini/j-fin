package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.Tuple2;
import liquibase.exception.LiquibaseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import uk.co.mr.finance.domain.StatementSummary;
import uk.co.mr.finance.runner.StatementLoaderRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.co.mr.finance.load.UtilForTest.createDatabase;

public class StatementPathLoaderRunnerTest {
  private static final Logger LOG = LoggerFactory.getLogger(StatementPathLoaderRunnerTest.class);

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

  private static FileSystem fileSystem;

  @BeforeAll
  public static void setup() throws SQLException, LiquibaseException {
    LOG.info("Starting up");
    container.start();
    Connection connection = DriverManager.getConnection(container.getJdbcUrl(), USER_ID, "finance");

    createDatabase(connection);

    fileSystem = Jimfs.newFileSystem(Configuration.windows());
  }

  @Test
  @DisplayName("All parameters are null")
  public void test_runner_null_parameters() {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    assertThrows(NullPointerException.class, runner::call);
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

    Collection<? extends Tuple2<Optional<Throwable>, ? extends Optional<?>>> call = runner.call();
    Optional<? extends Tuple2<Optional<Throwable>, ? extends Optional<?>>> anyTuple = call.stream().findAny();
    assertThat(call.size(), is(equalTo(1)));
    assertThat(anyTuple.flatMap(Tuple2::_2), is(Optional.empty()));

    Optional<Throwable> maybeThrowable = anyTuple.map(Tuple2::_1)
                                                 .flatMap(o -> o);

    maybeThrowable.ifPresentOrElse(e -> assertThat(e, instanceOf(IOException.class)),
                                   () -> fail("No throwable present as first element of the tuple and one was expected"));

    maybeThrowable.map(Throwable::getMessage)
                  .ifPresentOrElse(e -> assertThat(e, is("File [D:\\Development\\DevProjects\\j-fin\\non-existent-file] cannot be read")),
                                   () -> fail("Throwable message is different from the expected"));
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

    Collection<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> call = runner.call();
    Optional<Tuple2<Optional<Throwable>, Optional<StatementSummary>>> anyTuple = call.stream().findAny();
    assertThat(call.size(), is(equalTo(1)));
    assertThat(anyTuple.flatMap(Tuple2::_1), is(Optional.empty()));

    Optional<StatementSummary> maybeSummary = anyTuple.map(Tuple2::_2).flatMap(o -> o);

    maybeSummary.map(StatementSummary::getCount)
                .ifPresentOrElse(count -> assertThat(count, is(equalTo(1L))),
                                 () -> fail("There is no summary when one was expected"));


    maybeSummary.map(StatementSummary::totalAmount)
                .ifPresentOrElse(count -> assertThat(count, is(equalTo(new BigDecimal("-114.00")))),
                                 () -> fail("There is no summary when one was expected"));
  }
}