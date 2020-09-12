package uk.co.mr.finance.load;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Savepoint;


public class DatabaseManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);
  private static final int MAX_POOL_SIZE = 4;

  private final HikariDataSource hikariDataSource;

  public static DatabaseManager from(String driverName, String connectString, String userId, String userPwd) {
    return new DatabaseManager(driverName, connectString, userId, userPwd);
  }

  private DatabaseManager(String driverName, String connectString, String userId, String userPwd) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(connectString);
    hikariConfig.setDriverClassName(driverName);
    hikariConfig.setUsername(userId);
    hikariConfig.setPassword(userPwd);
    hikariConfig.setAutoCommit(false);
    hikariConfig.setMaximumPoolSize(MAX_POOL_SIZE);

    hikariDataSource = new HikariDataSource(hikariConfig);
  }

  public final Try<Connection> getConnection() {
    LOG.warn(">>>Trying to get get a connection, Max Pool:[{}], Max Life time:[{}]", hikariDataSource.getMaximumPoolSize(),hikariDataSource.getMaxLifetime());
    return Try.of(hikariDataSource::getConnection);
  }

  public void safeClose() {
    hikariDataSource.close();
  }

  public static Option<Savepoint> safeSetSavePoint(Connection connection) {
    return Try.of(() -> connection)
              .peek(c -> LOG.debug("About to set save point to connection:[{}]",c))
              .peek(c -> LOG.info("About to set save point to connection:[{}]",c))
              .mapTry(Connection::setSavepoint)
              .onFailure(e -> LOG.error("Exception while setting save point in connection", e))
              .onSuccess(s -> LOG.debug("Save point:[{}]",s))
              .toOption();
  }

  public static void safeCloseConnection(Connection connection) {
    LOG.warn(">>>Trying to close connection:[{}]", connection);
    Try.run(connection::close)
       .onFailure(e -> LOG.error("Exception while closing connection", e));
  }

  public static void safeCommit(Connection connection) {
    Try.run(connection::commit)
       .onFailure(e -> LOG.error("Exception while committing connection", e));
  }

  public static void safeRollback(Connection connection) {
    Try.run(connection::rollback)
       .onFailure(e -> LOG.error("Exception while rolling back connection", e));
  }

  public static void safeRollbackTo(Connection connection, Savepoint savePoint) {
    Try.run(() -> connection.rollback(savePoint))
       .onFailure(e -> LOG.error("Exception while rolling back connection to save point:[{}]", savePoint, e));
  }

}
