package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.control.Either;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import uk.co.mr.finance.domain.LoadControl;
import uk.co.mr.finance.domain.Statement;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.co.mr.finance.db.Tables.LOAD_CONTROL;
import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;
import static uk.co.mr.finance.load.UtilForTest.createDatabase;

public class AllOkStatementLoaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(AllOkStatementLoaderTest.class);


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
    private static DatabaseManager databaseManager;

    @BeforeAll
    public static void set_up() throws SQLException, LiquibaseException, IOException {
        LOG.info("Starting up");
        container.start();

        connection = DriverManager.getConnection(container.getJdbcUrl(), USER_ID, "finance");
        databaseManager = new DatabaseManager(connection);

        createDatabase(connection);

        fileSystem = Jimfs.newFileSystem(Configuration.windows());

        Collection<Path> filesToLoad = createFilesToLoad();

        StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
        List<Pair<Path, Either<Throwable, StatementSummary>>> pairs = loadFiles(loader, filesToLoad);
        pairs.forEach(p -> LOG.info("Results:[{}]:", p));
    }

    private static Collection<Path> createFilesToLoad() throws IOException {
        String file1Content = """
                Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
                29/04/2020,DD,'11-22-33,87651234,NATIONAL TRUST FOR,114.00,,6941.03
                29/04/2020,DD,'11-22-33,87651234,COUNTRYWIDE PS HH,22.69,,7055.03
                28/04/2020,DEB,'11-22-33,87651234,Transferwise Ltd,500.00,,7077.72
                28/04/2020,DD,'11-22-33,87651234,E.ON,76.00,,7577.72
                27/04/2020,SO,'11-22-33,87651234,XXX YYY (RE,500.00,,7653.72
                27/04/2020,DEB,'11-22-33,87651234,AMZNMKTPLACE AMAZO,19.99,,8153.72
                """;
        Path path1 = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);

        String file2Content = """
                Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
                11/05/2020,DEB,'11-22-33,87651234,LIDL GB GLASGOW,51.35,,5489.85
                11/05/2020,DEB,'11-22-33,87651234,NOWTV.COM/BILLINGH,7.19,,5541.20
                07/05/2020,DEB,'11-22-33,87651234,AMZNMKTPLACE AMAZO,49.97,,5548.39
                07/05/2020,DEB,'11-22-33,87651234,NETFLIX.COM,8.99,,5598.36
                06/05/2020,DEB,'11-22-33,87651234,WM MORRISONS STORE,62.17,,5607.35
                06/05/2020,DD,'11-22-33,87651234,SKY DIGITAL,54.13,,5669.52
                05/05/2020,DEB,'11-22-33,87651234,AMAZON.CO.UK*CS8LJ,40.00,,5723.65
                05/05/2020,DD,'11-22-33,87651234,H3G,20.50,,5763.65                
                                """;
        Path path2 = UtilForTest.createFile(fileSystem, "extrato_02.csv", file2Content);

        String file3Content = """
                Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
                18/03/2020,DEB,'11-22-33,87651234,LIDL GB GLASGOW,29.51,,3812.09
                18/03/2020,DD,'11-22-33,87651234,PURE GYM LTD,18.99,,3841.60
                17/03/2020,DEB,'11-22-33,87651234,MRH ROAD TO THE IS,39.98,,3860.59
                16/03/2020,DEB,'11-22-33,87651234,WM MORRISONS STORE,50.47,,3900.57                                
                                """;
        Path path3 = UtilForTest.createFile(fileSystem, "extrato_03.csv", file3Content);

        return List.of(path1, path2, path3);
    }

    private static List<Pair<Path, Either<Throwable, StatementSummary>>> loadFiles(StatementLoader loader, Collection<? extends Path> filesToLoad) {
        Function<Path, Pair<Path, Either<Throwable, StatementSummary>>> mapper =
                path -> Pair.of(path, loader.load(path, Statement.transformToStatement()));
        return filesToLoad.stream()
                          .map(mapper)
                          .collect(Collectors.toList());
    }

    @AfterAll
    public static void tear_down() {
        LOG.info("Shutting down");
        container.stop();
    }


    @Test
    @DisplayName("Ensure all rows are loaded")
    public void check_row_counter() {
        assertThat(getCounter(), is(18));
    }

    @Test
    @DisplayName("Ensure transaction order has neither gaps nor duplication and matches counter")
    public void statement_ata_content_check() {
        Integer counter = getCounter();
        DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        List<Statement> records =
                ctx.selectFrom(STATEMENT_DATA)
                   .orderBy(STATEMENT_DATA.STATEMENT_DATE, STATEMENT_DATA.STATEMENT_ID.desc())
                   .fetch()
                   .into(Statement.class);

        int quantity = 0;
        for (Statement record : records) {
            Integer order = record.transactionOrder();
            assertThat(order, notNullValue());
            quantity++;
            assertThat(order, is(quantity));
        }
        assertThat("count(*) and row iteration count does not match", quantity, is(counter));
    }

    @Test
    @DisplayName("Ensure there are rows in correct state in load control table")
    public void check_load_control() {
        DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        List<LoadControl> records = ctx.selectFrom(LOAD_CONTROL).fetch().into(LoadControl.class);
        assertThat(records.size(), is(3));

        records.stream()
               .peek(record -> LOG.info("Record:[{}]", record))
               .filter(record -> record.loadInProgress().equals(Boolean.TRUE))
               .findAny()
               .ifPresent(record -> {
                   throw new IllegalArgumentException("Expecting to have no rows in processing state");
               });

        LoadControl lcr1 = loadedRecordCountFor(records.stream(), 1);
        assertThat(lcr1.loadedRecords(), is(6));
        assertThat(lcr1.fileName(), notNullValue());
        assertThat(lcr1.loadDate(), equalTo(LocalDate.now()));
        assertThat(lcr1.smallestDate(), equalTo(LocalDate.of(2020, 4, 27)));
        assertThat(lcr1.greatestDate(), equalTo(LocalDate.of(2020, 4, 29)));


        LoadControl lcr2 = loadedRecordCountFor(records.stream(), 2);
        assertThat(lcr2.loadedRecords(), is(8));
        assertThat(lcr2.fileName(), notNullValue());
        assertThat(lcr2.loadDate(), equalTo(LocalDate.now()));
        assertThat(lcr2.smallestDate(), equalTo(LocalDate.of(2020, 5, 5)));
        assertThat(lcr2.greatestDate(), equalTo(LocalDate.of(2020, 5, 11)));

        LoadControl lcr3 = loadedRecordCountFor(records.stream(), 3);
        assertThat(lcr3.loadedRecords(), is(4));
        assertThat(lcr3.fileName(), notNullValue());
        assertThat(lcr3.loadDate(), equalTo(LocalDate.now()));
        assertThat(lcr3.smallestDate(), equalTo(LocalDate.of(2020, 3, 16)));
        assertThat(lcr3.greatestDate(), equalTo(LocalDate.of(2020, 3, 18)));
    }

    private LoadControl loadedRecordCountFor(Stream<LoadControl> records, int controlId) {
        return records.filter(r -> r.controlId() == controlId)
                      .reduce((a, b) -> {
                          throw new IllegalArgumentException(
                                  "Expecting to have at most 1 element for controlId:[%s]".formatted(controlId));
                      })
                      .orElseThrow(() -> {
                          throw new IllegalArgumentException(
                                  "Expecting to have at least 1 element for controlId:[%s]".formatted(controlId));
                      });
    }

    private Integer getCounter() {
        DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        return ctx.selectCount().from(STATEMENT_DATA).fetchOne(0, int.class);
    }
}
