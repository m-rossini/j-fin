package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.Tuple2;
import liquibase.exception.LiquibaseException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import uk.co.mr.finance.domain.LoadControl;
import uk.co.mr.finance.domain.Statement;
import uk.co.mr.finance.exception.LoaderException;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.co.mr.finance.db.Tables.LOAD_CONTROL;
import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

public class NotOkStatementLoaderTest {
  private static final Logger LOG = LoggerFactory.getLogger(NotOkStatementLoaderTest.class);


  @Container
  private static final PostgreSQLContainer container =
      new PostgreSQLContainer<>("postgres:latest")
          .withDatabaseName("finance")
          .withUsername("finance")
          .withPassword("finance")
          .withExposedPorts(5432);
  private static FileSystem fileSystem;
  private Connection connection;
  private DSLContext ctx;

  @BeforeAll
  public static void set_up() {
    LOG.info("Starting up");
    container.start();

    fileSystem = Jimfs.newFileSystem(Configuration.windows());

  }


  @AfterAll
  public static void tear_down() {
    LOG.info("Shutting down");
    container.stop();
  }

  @BeforeEach
  public void create() throws LiquibaseException, SQLException {
    connection = DriverManager.getConnection(container.getJdbcUrl(), "finance", "finance");
    UtilForTest.createDatabase(connection);

    ctx = DSL.using(connection, SQLDialect.POSTGRES);
  }

  @AfterEach()
  public void dropAll() throws LiquibaseException, SQLException {
    connection.rollback();
    UtilForTest.dropDatabase(connection);
    connection.close();
  }

  @Test
  @DisplayName("Load data that is too large")
  public void test_large_data() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        29/04/2020,DD,'11-22-33,12345678901234567890,NATIONAL TRUST FOR,114.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load = loader.load(path, Statement.transformToStatement());

    assertNotNull(load);
    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().get(), instanceOf(DataAccessException.class));
    assertThat(load._1().get().getCause(), instanceOf(PSQLException.class));
    assertThat(load._1().get().getCause().getMessage(),
               is(equalTo("ERROR: value too long for type character varying(15)")));

    assertThat(load._2(), is(Optional.empty()));

    Files.delete(path);
  }

  @Test
  @DisplayName("Try to load wrong big decimal")
  public void load_wrong_amount() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        03/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR3,40.x0,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load =
        loader.load(path, Statement.transformToStatement());

    assertThat(getStatementCounter(), is(equalTo(0)));
    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().filter(NumberFormatException.class::isInstance).isPresent(), is(equalTo(true)));
    load._1()
        .map(Throwable::getMessage)
        .ifPresentOrElse(o -> assertThat(o,
                                         is(equalTo(
                                             "Character x is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark."))),
                         () -> fail("Expecting to be a throwable with a message"));

    assertThat(load._2().isEmpty(), is(equalTo(true)));

    Files.delete(path);
  }

  @Test
  @DisplayName("Try to load wrong date")
  public void load_wrong_date() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        16/16/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR3,40.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load =
        loader.load(path, Statement.transformToStatement());


    assertThat(load._1().isPresent(), is(equalTo(true)));

    assertThat(load._1().get(), instanceOf(DateTimeParseException.class));
    assertThat(load._1().get().getMessage(),
               is(equalTo(
                   "Text '16/16/2020' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 16")));

    assertThat(load._2(), is(Optional.empty()));

    Files.delete(path);
  }

  @Test
  @DisplayName("Load duplicate Rows")
  public void load_duplicated_row() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        29/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR,114.00,,6941.03
        29/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR,114.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));

    assertThat(summary.getCount(), is(1L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 29)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 29)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-114.00")));
    assertThat(getStatementCounter(), is(1));

    Files.delete(path);
  }

  @Test
  @DisplayName("Try to load 3, 1st is wrong, should load 2")
  public void load_2_out_of_3() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        01/04/2020,DD,'11-22-33,12345678901234567890,NATIONAL TRUST FOR1,10.00,,6941.03
        02/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR2,20.00,,6941.03
        03/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR3,40.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));

    assertThat(summary.getCount(), is(2L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 2)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-60.00")));
    assertThat(getStatementCounter(), is(2));

    Files.delete(path);
  }

  @Test
  @DisplayName("Try to load 3, 1st and 2nd are wrong, should load 1")
  public void load_1_out_of_3() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        01/04/2020,DD,'11-22-33,12345678901234567890,NATIONAL TRUST FOR1,10.00,,6941.03
        02/04/2020,DD,'11-22-330000000,87651234,NATIONAL TRUST FOR2,20.00,,6941.03
        03/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR3,40.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));

    assertThat(summary.getCount(), is(1L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-40.00")));
    assertThat(getStatementCounter(), is(1));

    Files.delete(path);
  }

  @Test
  @DisplayName("Try to load 3, all wrong should load none")
  public void load_0_out_of_3() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        01/04/2020,DD,'11-22-33,12345678901234567890,NATIONAL TRUST FOR1,10.00,,6941.03
        02/04/2020,DD,'11-22-330000000,87651234,NATIONAL TRUST FOR2,20.00,,6941.03
        03/04/2020,DDDDDDDDDD,'11-22-33,87651234,NATIONAL TRUST FOR3,40.00,,6941.03
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load = loader.load(path, Statement.transformToStatement());
    Files.delete(path);

    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().get(), instanceOf(DataAccessException.class));
    assertThat(load._1().get().getCause(), instanceOf(PSQLException.class));
    assertThat(load._1().get().getCause().getMessage(), is(equalTo("ERROR: value too long for type character varying(15)")));
    assertThat(load._2(), is(Optional.empty()));

    assertThat(getStatementCounter(), is(0));

    List<LoadControl> controls = getLoadControl();
    assertThat(controls.size(), is(equalTo(1)));

    LoadControl loadControl = controls.get(0);
    assertThat(loadControl.controlId(), is(equalTo(1)));
    assertThat(loadControl.fileHash(), is(equalTo("32c08cca3540dbe2fbaa7ee9482570c0")));
    assertThat(loadControl.fileName(), is(equalTo(path.toAbsolutePath().toString())));
    //TODO FINISH Testing all Columns
    assertThat(loadControl.loadInProgress(), is(equalTo(false)));

  }

  private List<LoadControl> getLoadControl() {
    return ctx.selectFrom(LOAD_CONTROL)
              .fetch()
              .into(LoadControl.class)
        ;
  }

  private Integer getStatementCounter() {
    return ctx.selectCount().from(STATEMENT_DATA).fetchOne(0, int.class);
  }
}
