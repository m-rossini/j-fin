package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.org.apache.commons.lang.SystemUtils;
import uk.co.mr.finance.domain.StatementSummary;
import uk.co.mr.finance.exception.LoaderException;
import uk.co.mr.finance.runner.StatementLoaderRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

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
  public static void setup() throws SQLException {
    LOG.info("Starting up");
    container.start();
    Connection connection = DriverManager.getConnection(container.getJdbcUrl(), USER_ID, "finance");

    createDatabase(connection);

    if (SystemUtils.IS_OS_WINDOWS) {
      fileSystem = Jimfs.newFileSystem(Configuration.windows());
    } else if (SystemUtils.IS_OS_UNIX) {
      fileSystem = Jimfs.newFileSystem(Configuration.unix());
    } else if (SystemUtils.IS_OS_MAC_OSX) {
      fileSystem = Jimfs.newFileSystem(Configuration.osX());
    } else {
      fileSystem = Jimfs.newFileSystem();
    }

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

    getField("driverName").map(setFieldValue(runner, container.getDriverClassName()))
                          .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("connectString").map(setFieldValue(runner, container.getJdbcUrl()))
                             .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("userId").map(setFieldValue(runner, "finance"))
                      .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("cleanPassword").map(setFieldValue(runner, "finance"))
                             .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    Path path = Paths.get("non-existent-file");
    getField("toLoadPath").map(setFieldAccessible())
                          .map(setFieldValue(runner, path))
                          .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    Collection<? extends Tuple2<Optional<Throwable>, ? extends Optional<?>>> call = runner.call();
    Optional<? extends Tuple2<Optional<Throwable>, ? extends Optional<?>>> anyTuple = call.stream().findAny();
    assertThat(call.size(), is(equalTo(1)));
    assertThat(anyTuple.flatMap(Tuple2::_2), is(Optional.empty()));

    Optional<Throwable> maybeThrowable = anyTuple.map(Tuple2::_1)
                                                 .flatMap(o -> o);

    if (maybeThrowable.isPresent()) {
      assertThat(maybeThrowable.get(), instanceOf(IOException.class));
    } else {
      fail("No throwable present as first element of the tuple and one was expected");
    }

    if (maybeThrowable.map(Throwable::getMessage).isPresent()) {
      assertThat(maybeThrowable.map(Throwable::getMessage).get(), is("File [" + path.toAbsolutePath() + "] cannot be read"));
    } else {
      fail("Throwable message is different from the expected");
    }
  }

  @NotNull
  private <T> Function<Field, Field> setFieldValue(StatementLoaderRunner runner, T value) {
    return f -> {
      try {
        f.set(runner, value);
      } catch (IllegalAccessException e) {
        throw new LoaderException("Unable to set value for field private variable");
      }
      return f;
    };
  }

  @NotNull
  private Function<Field, Field> setFieldAccessible() {
    return f -> {
      f.setAccessible(true);
      return f;
    };
  }

  private Optional<Field> getField(String fieldName) {
    return ReflectionUtils.findFields(StatementLoaderRunner.class,
                                      f -> f.getName().equals(fieldName),
                                      ReflectionUtils.HierarchyTraversalMode.BOTTOM_UP)
                          .stream()
                          .reduce((a, b) -> {
                            throw new RuntimeException("Should find only one field and more than one was found");
                          });
  }

  @Test
  @DisplayName("All Informed and should run Ok")
  public void test_runner_all_informed() throws Throwable {
    StatementLoaderRunner runner = new StatementLoaderRunner();

    getField("driverName").map(setFieldValue(runner, container.getDriverClassName()))
                          .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("connectString").map(setFieldValue(runner, container.getJdbcUrl()))
                             .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("userId").map(setFieldValue(runner, "finance"))
                      .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    getField("cleanPassword").map(setFieldValue(runner, "finance"))
                             .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));

    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        29/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR,114.00,,6941.03
        """;
    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    getField("toLoadPath").map(setFieldAccessible())
                          .map(setFieldValue(runner, path))
                          .orElseThrow(() -> new RuntimeException("Was expecting at least one field and none was found"));


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