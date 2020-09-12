package uk.co.mr.finance.load;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.sql.Connection;
import java.util.Optional;

import static uk.co.mr.finance.load.UtilForTest.DRIVER_NAME;

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

  private DatabaseManager databaseManager;
  private DSLContext ctx;

  public Optional<Connection> getConnection() {
    return databaseManager.getConnection().toJavaOptional();
  }

  public Optional<DatabaseManager> getDatabaseManager() {
    return Optional.ofNullable(databaseManager);
  }

  public Optional<DSLContext> getDatabaseContext() {
    return Optional.ofNullable(ctx);
  }

  public void start() {
    container.start();
    LOG.info("Container has started as [{}:{}]", container.getContainerInfo().getName(), container.getFirstMappedPort());

    databaseManager = DatabaseManager.from(DRIVER_NAME, container.getJdbcUrl(), container.getUsername(), container.getPassword());
    ctx = this.databaseManager.getConnection()
                              .map(c -> DSL.using(c, SQLDialect.POSTGRES))
                              .getOrElseThrow(() -> new IllegalArgumentException("Connection is not created"));
  }

  public void stop() {
    ctx.close();
    databaseManager.safeClose();
    container.stop();
  }
}
