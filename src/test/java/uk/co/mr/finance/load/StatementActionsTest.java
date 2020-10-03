package uk.co.mr.finance.load;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.vavr.Value;
import io.vavr.collection.Seq;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import liquibase.exception.LiquibaseException;
import org.jetbrains.annotations.NotNull;
import org.jooq.Record11;
import org.jooq.ResultQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.jooq.impl.DSL.lead;
import static org.jooq.impl.DSL.orderBy;
import static org.jooq.impl.DSL.rowNumber;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

class StatementActionsTest {
  private static final Logger LOG = LoggerFactory.getLogger(StatementActionsTest.class);
  private static FileSystem fileSystem;
  private static ContainerManager containerManager;

  @BeforeAll
  public static void create_environment() throws SQLException, LiquibaseException {
    containerManager = new ContainerManager();
    containerManager.start();
    containerManager.getConnection().ifPresent(UtilForTest::createDatabase);

    fileSystem = Jimfs.newFileSystem(Configuration.windows());
  }

  @AfterAll
  public static void destroy_environment() throws SQLException, IOException {
    fileSystem.close();
    containerManager.stop();
  }

  @Test
  @DisplayName("Insert and then create statement order correctly")
  public void try_to_reorder_data() throws IOException {
    StatementActions actions = containerManager.getDatabaseContext()
                                               .map(StatementActions::new)
                                               .orElseThrow(() -> new RuntimeException("There is no Database context available"));

    String shouldBeOrder = "should_be_order";
    loadFirstFile(actions);
    String previousBalanceColumn = "previous_balance";
    long expectedSize = 6L;

    List<Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal>> rows1 =
        getRecords(shouldBeOrder, previousBalanceColumn);
    long size1 = rows1.stream()
                      .peek(r -> assertNull(r.get(STATEMENT_DATA.TRANSACTION_ORDER)))
                      .peek(r -> balanceCheck(previousBalanceColumn, r))
                      .peek(r -> loadOrderCheck(shouldBeOrder, (int) expectedSize, r))
                      .count();
    assertThat(expectedSize, is(equalTo(size1)));

    actions.tryReorderData();
    containerManager.getDatabaseManager()
                    .map(DatabaseManager::getConnection)
                    .flatMap(Value::toJavaOptional)
                    .ifPresent(DatabaseManager::safeCommit);

    List<Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal>> rows2 =
        getRecords(shouldBeOrder, previousBalanceColumn);
    long size2 = rows2.stream()
                      .peek(r -> assertNotNull(r.get(STATEMENT_DATA.TRANSACTION_ORDER)))
                      .peek(r -> balanceCheck(previousBalanceColumn, r))
                      .peek(r -> loadOrderCheck(shouldBeOrder, (int) expectedSize, r))
                      .peek(r -> transactionOrderCheck(expectedSize, r))
                      .count();
    assertThat(expectedSize, is(equalTo(size2)));
  }

  private void transactionOrderCheck(long expectedSize, Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal> r) {
    Integer transactionOrder = r.get(STATEMENT_DATA.TRANSACTION_ORDER, Integer.class);
    Integer loadOrder = r.get(STATEMENT_DATA.STATEMENT_ID);
    assertThat(transactionOrder + loadOrder, is(equalTo((int) expectedSize + 1)));
  }

  private void loadOrderCheck(String shouldBeOrder, int expectedSize, Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal> r) {
    Integer shouldBe = r.get(shouldBeOrder, Integer.class);
    Integer loadOrder = r.get(STATEMENT_DATA.STATEMENT_ID);
    assertThat(shouldBe + loadOrder, is(equalTo(expectedSize + 1)));
  }

  private void balanceCheck(String previousBalanceColumn, Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal> r) {
    BigDecimal previousBalance = r.get(previousBalanceColumn, BigDecimal.class);
    if (previousBalance != null) {
      BigDecimal amount = r.get(STATEMENT_DATA.TRANSACTION_AMOUNT);
      BigDecimal balance = r.get(STATEMENT_DATA.TOTAL_BALANCE);
      assertThat(previousBalance.add(amount), is(equalTo(balance)));
    }
  }

  @NotNull
  private List<Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal>> getRecords(String shouldBeOrder, String previousBalance) {
    Stream<Record11<Integer, Integer, Integer, LocalDate, String, String, String, String, BigDecimal, BigDecimal, BigDecimal>> rows =
        containerManager.getDatabaseContext()
                        .map(c -> c.select(STATEMENT_DATA.STATEMENT_ID,
                                           STATEMENT_DATA.TRANSACTION_ORDER,
                                           rowNumber().over().orderBy(STATEMENT_DATA.STATEMENT_ID.desc()).as(shouldBeOrder),
                                           STATEMENT_DATA.STATEMENT_DATE,
                                           STATEMENT_DATA.TRANSACTION_TYPE,
                                           STATEMENT_DATA.SORT_CODE,
                                           STATEMENT_DATA.ACCOUNT_ID,
                                           STATEMENT_DATA.TRANSACTION_DESCRIPTION,
                                           STATEMENT_DATA.TRANSACTION_AMOUNT,
                                           STATEMENT_DATA.TOTAL_BALANCE,
                                           lead(STATEMENT_DATA.TOTAL_BALANCE).over(orderBy(STATEMENT_DATA.STATEMENT_ID)).as(previousBalance))
                                   .from(STATEMENT_DATA))
                        .map(ResultQuery::fetch)
                        .stream()
                        .flatMap(Collection::stream);

    return rows.collect(Collectors.toList());
  }

  private void loadFirstFile(StatementActions actions) throws IOException {
    String file1Content = """
        Transaction Date,Transaction Type,Sort Code,Account Number,Transaction Description,Debit Amount,Credit Amount,Balance
        29/04/2020,DD,'11-22-33,12,DesA,7,,7
        29/04/2020,DD,'11-22-33,12,DesB,6,,14
        28/04/2020,DEP,'11-22-33,12,DesC,,15,20
        28/04/2020,DD,'11-22-33,12,DesD,3,,5
        27/04/2020,SO,'11-22-33,12,DesE,2,,8
        27/04/2020,DEB,'11-22-33,12,DesF,,5,10
        """;
    Path path = UtilForTest.createFile(fileSystem, "extrato_01.csv", file1Content);

    InputDataManager inputDataManager = new InputDataManager();
    Try<Stream<Validation<Seq<Throwable>, Statement>>> tryStatement = inputDataManager.transformFile(path, Statement.transformToStatement());
    assertTrue(tryStatement.isSuccess());
    List<Statement> statements = tryStatement.get()
                                             .filter(Validation::isValid)
                                             .map(Validation::get)
                                             .collect(Collectors.toList());
    assertThat(statements.size(), is(equalTo(6)));

    long successInserts = statements.stream()
                                    .map(actions::tryInsertIntoStatement)
                                    .filter(Try::isSuccess)
                                    .count();
    assertThat(successInserts, is(equalTo(6L)));

    containerManager.getDatabaseManager()
                    .map(DatabaseManager::getConnection)
                    .flatMap(Value::toJavaOptional)
                    .ifPresent(DatabaseManager::safeCommit);
  }
}