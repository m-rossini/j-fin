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
import uk.co.mr.finance.domain.StatementSummary;
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load = loader.load(path, Statement.transformToStatement());

    Files.delete(path);

    assertNotNull(load);
    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().get(), instanceOf(DataAccessException.class));
    assertThat(load._1().get().getCause(), instanceOf(PSQLException.class));
    assertThat(load._1().get().getCause().getMessage(), is(equalTo("ERROR: value too long for type character varying(15)")));

    assertThat(load._2(), is(Optional.empty()));

    checkLoadControl(1, 1, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(0), 0, false, path, "bfcc0fae96900c92a252ecb22ce1e432");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load =
        loader.load(path, Statement.transformToStatement());

    Files.delete(path);

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

    checkLoadControl(1, 1, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(0), 0, false, path, "baf8463fccca649d1a0bf127b4d6d2fc");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load =
        loader.load(path, Statement.transformToStatement());

    Files.delete(path);

    assertThat(load._1().isPresent(), is(equalTo(true)));

    assertThat(load._1().get(), instanceOf(DateTimeParseException.class));
    assertThat(load._1().get().getMessage(),
               is(equalTo(
                   "Text '16/16/2020' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 16")));

    assertThat(load._2(), is(Optional.empty()));

    checkLoadControl(1, 1, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(0), 0, false, path, "62889ee3d54bc2be72e3a5ad735957e");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));
    Files.delete(path);

    assertThat(summary.getCount(), is(1L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 29)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 29)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-114.00")));
    assertThat(getStatementCounter(), is(1));

    checkLoadControl(1, 1, LocalDate.of(2020, 04, 29), LocalDate.of(2020, 04, 29), 1, false, path, "cc21afc68732dee812b2a3e010272ad");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));
    Files.delete(path);

    assertThat(summary.getCount(), is(2L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 2)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-60.00")));
    assertThat(getStatementCounter(), is(2));

    checkLoadControl(1, 1, LocalDate.of(2020, 04, 02), LocalDate.of(2020, 04, 03), 2, false, path, "4c4251ff0e182e9c28f3ef84ebd21fd0");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    StatementSummary summary = loader.load(path, Statement.transformToStatement())._2()
                                     .orElseThrow(() -> new LoaderException("Should have a summary"));
    Files.delete(path);

    assertThat(summary.getCount(), is(1L));
    assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 3)));
    assertThat(summary.totalAmount(), is(new BigDecimal("-40.00")));
    assertThat(getStatementCounter(), is(1));

    checkLoadControl(1, 1, LocalDate.of(2020, 04, 03), LocalDate.of(2020, 04, 03), 1, false, path, "6c2b65d0bc51f75fb23a5fe50c96994");
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
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load = loader.load(path, Statement.transformToStatement());
    Files.delete(path);

    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().get(), instanceOf(DataAccessException.class));
    assertThat(load._1().get().getCause(), instanceOf(PSQLException.class));
    assertThat(load._1().get().getCause().getMessage(), is(equalTo("ERROR: value too long for type character varying(15)")));
    assertThat(load._2(), is(Optional.empty()));

    assertThat(getStatementCounter(), is(0));

    checkLoadControl(1, 1, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(0), 0, false, path, "32c08cca3540dbe2fbaa7ee9482570c0");
  }

  @Test
  @DisplayName("Load 1 file successfully and try to load it again")
  public void try_to_load_an_already_loaded_file() throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        02/04/2020,DD,'11-22-33,1234,NATIONAL TRUST FOR2,,1,2
        1
        01/04/2020,DD,'11-22-33,1234,NATIONAL TRUST FOR1,1,,1
        """;

    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);
    Files.readAllLines(path).forEach(line -> LOG.info("Line:[{}]", line));

    DatabaseManager databaseManager = new DatabaseManager(connection);
    StatementLoader loader = new StatementLoader(databaseManager, new FileManager(), new LoadControlActions(ctx), new StatementActions(ctx));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load = loader.load(path, Statement.transformToStatement());
    Files.delete(path);

    assertThat(load._1().isPresent(), is(equalTo(true)));
    assertThat(load._1().get(), instanceOf(LoaderException.class));
    assertThat(load._1().get().getMessage(),
               is(equalTo("Record [1] does not have all needed fields to be transformed to statement")));
    assertThat(load._2().isPresent(), is(equalTo(true)));
    assertThat(load._2().get().count(), is(equalTo(2L)));
    assertThat(load._2().get().minDate(), is(equalTo(LocalDate.of(2020, 04, 01))));
    assertThat(load._2().get().maxDate(), is(equalTo(LocalDate.of(2020, 04, 02))));
    assertThat(load._2().get().totalAmount(), is(equalTo(BigDecimal.ZERO)));
    assertThat(getStatementCounter(), is(2));
    checkLoadControl(1, 1, LocalDate.of(2020, 04, 01), LocalDate.of(2020, 04, 02), 2, false, path, "badad3271238b20adef511aad2efc238");

    Path path2 = UtilForTest.createFile(fileSystem, "extrato_02.csv", file1Content);
    Files.readAllLines(path2).forEach(line -> LOG.info("Line(path2):[{}]", line));
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load2 = loader.load(path2, Statement.transformToStatement());
    Files.delete(path2);

    assertThat(load2._1().isPresent(), is(equalTo(true)));
    assertThat(load2._1().get(), instanceOf(LoaderException.class));
    assertThat(load2._1().get().getMessage(),
               is(equalTo("hashCode [badad3271238b20adef511aad2efc238] already loaded in file:[C:\\work\\extrato_01.csv]")));
    assertThat(load2._2().isPresent(), is(equalTo(false)));
    System.out.println();
  }

  private void checkLoadControl(int loadControlRows,
                                int controlRowId,
                                LocalDate smallestDate,
                                LocalDate greatestDate,
                                int totalRecords,
                                boolean isThereLoadInProgress,
                                Path filePath,
                                String fileHashCode) {
    List<LoadControl> controls = getLoadControl();
    assertThat(controls.size(), is(equalTo(loadControlRows)));

    LoadControl loadControl = controls.get(0);
    assertThat(loadControl.controlId(), is(equalTo(controlRowId)));
    assertThat(loadControl.loadDate(), is(equalTo(LocalDate.now())));
    assertThat(loadControl.smallestDate(), is(equalTo(smallestDate)));
    assertThat(loadControl.greatestDate(), is(equalTo(greatestDate)));
    assertThat(loadControl.loadedRecords(), is(equalTo(totalRecords)));
    assertThat(loadControl.loadInProgress(), is(equalTo(isThereLoadInProgress)));
    assertThat(loadControl.fileName(), is(equalTo(filePath.toAbsolutePath().toString())));
    assertThat(loadControl.fileHash(), is(equalTo(fileHashCode)));
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

