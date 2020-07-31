package uk.co.mr.finance.load;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

public class ContainerManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
  private static final String USER_ID = "finance";
  private static final String PASS_WORD = "finance";
  private static final String DATABASE_NAME = "finance";

  @Container
  private static final PostgreSQLContainer container =
      new PostgreSQLContainer<>("postgres:latest")
          .withDatabaseName(DATABASE_NAME)
          .withUsername(USER_ID)
          .withPassword(PASS_WORD)
          .withExposedPorts(5432);

  private Connection connection;
  private DatabaseManager databaseManager;
  private DSLContext ctx;

  public JdbcDatabaseContainer getContainer() {
    return container;
  }

  public Optional<Connection> getConnection() {
    return Optional.ofNullable(connection);
  }

  public Optional<DatabaseManager> getDatabaseManager() {
    return Optional.ofNullable(databaseManager);
  }

  public Optional<DSLContext> getDatabaseContext() {
    return Optional.ofNullable(ctx);
  }

  public void start() throws SQLException {
    container.start();
    LOG.info("Container has started as [{}:{}]", container.getContainerInfo().getName(), container.getFirstMappedPort());

    connection = DriverManager.getConnection(container.getJdbcUrl(), USER_ID, "finance");
    databaseManager = new DatabaseManager(connection);
    ctx = databaseManager.getConnection()
                         .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                         .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"));
  }

  public void stop() throws SQLException {
    ctx.close();
    databaseManager.safeClose();
    connection.close();
    container.stop();
  }
}
