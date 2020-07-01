package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.control.Either;
import liquibase.exception.LiquibaseException;
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
import uk.co.mr.finance.domain.Statement;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
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
    private static Connection connection;
    private static FileSystem fileSystem;
    private static DatabaseManager databaseManager;

    @BeforeAll
    public static void set_up() throws SQLException, LiquibaseException, IOException {
        LOG.info("Starting up");
        container.start();

        connection = DriverManager.getConnection(container.getJdbcUrl(), "finance", "finance");
        databaseManager = new DatabaseManager(connection);

        UtilForTest.createDatabase(connection);
        fileSystem = Jimfs.newFileSystem(Configuration.windows());
    }


    @AfterAll
    public static void tear_down() {
        LOG.info("Shutting down");
        container.stop();
    }

    @Test
    @DisplayName("Load invalid data")
    public void test() throws IOException {
        String file1Content = """
                Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
                29/04/2020,DD,'11-22-33,12345678901234567890,NATIONAL TRUST FOR,114.00,,6941.03
                """;

        Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);

        StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
        Either<Throwable, StatementSummary> load = loader.load(path, Statement.transformToStatement());

        fail("To be finished");
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

        StatementLoader loader = new StatementLoader(databaseManager, new FileManager());
        StatementSummary summary = loader.load(path, Statement.transformToStatement())
                                         .getOrElseThrow((Function<Throwable, RuntimeException>) RuntimeException::new);
        assertThat(summary.getCount(), is(1L));
        assertThat(summary.minDate(), is(LocalDate.of(2020, 4, 29)));
        assertThat(summary.maxDate(), is(LocalDate.of(2020, 4, 29)));
        assertThat(summary.totalAmount(), is(new BigDecimal("-114.00")));
        assertThat(getCounter(), is(1));
    }

    private Integer getCounter() {
        DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        return ctx.selectCount().from(STATEMENT_DATA).fetchOne(0, int.class);
    }
}
