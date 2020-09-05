package uk.co.mr.finance.load;

import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Savepoint;
import java.util.Objects;

//TODO use a real connection pool please
public class DatabaseManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);

  private final Try<Connection> connection;

  public static final DatabaseManager from(String driverName, String connectString, String userId, String userPwd) {
    return new DatabaseManager(driverName, connectString, userId, userPwd);
  }

  public static final DatabaseManager from(Connection connection) {
    return new DatabaseManager(connection);
  }

  private DatabaseManager(String driverName, String connectString, String userId, String userPwd) {
    Objects.requireNonNull(driverName, "Driver name cannot be null");
    Objects.requireNonNull(connectString, "Connection string cannot be null");
    Objects.requireNonNull(userId, "User id and/or password cannot be null");
    Objects.requireNonNull(userPwd, "User id and/or password cannot be null");

    this.connection = connect(driverName, connectString, userId, userPwd);

  }

  private DatabaseManager(Connection connection) {
    this.connection =
        Try.of(() -> connection)
           .filterTry(c -> c.isValid(60))
           .onFailure(t -> LOG.error("Connection is not in a valid state", t));
  }

  private Try<Connection> connect(String driverName, String connectString, String userId, String userPwd) {
    return loadDriver(driverName).flatMap(v -> safeConnect(connectString, userId, userPwd));
  }

  private Try<? extends Class<?>> loadDriver(String driverName) {
    return Try.of(() -> Class.forName(driverName))
              .onFailure(e -> LOG.error("Unable to load Driver", e));
  }

  private Try<Connection> safeConnect(String connectString, String userId, String userPwd) {
    return Try.of(() -> DriverManager.getConnection(connectString, userId, userPwd))
              .onFailure(t -> LOG.error("Unable to obtain a connection", t));
  }

  public final Try<Connection> getConnection() {
    return connection;
  }

  public Option<Savepoint> safeSetSavePoint() {
    return connection.flatMap(c -> Try.of(c::setSavepoint)
                                      .onFailure(e -> LOG.error(
                                          "Exception while setting save point in connection", e)))
                     .toOption();
  }


  public void safeRollback() {
    connection.peek(c -> Try.run(c::rollback)
                            .onFailure(e -> LOG.error("Exception while rolling back connection", e)));
  }

  public void safeRollbackTo(Savepoint savePoint) {
    connection.peek(c -> Try.run(() -> c.rollback(savePoint))
                            .onFailure(e -> LOG.error("Exception while rolling back connection to save point:[{}]",
                                                      savePoint,
                                                      e)));
  }

  public void safeClose() {
    connection.peek(c -> Try.run(c::close).onFailure(e -> LOG.error("Exception while closing connection", e)));
  }

  public void safeCommit() {
    connection.peek(c -> Try.run(c::commit).onFailure(e -> LOG.error("Exception while committing connection", e)));
  }

  public void safeSetAutoCommitOff() {
    connection.peek(c -> Try.run(() -> c.setAutoCommit(false))
                            .onFailure(e -> LOG.error("Exception while setting autocommit on", e)));
  }

  public void safeSetAutoCommitOn() {
    connection.peek(c -> Try.run(() -> c.setAutoCommit(true))
                            .onFailure(e -> LOG.error("Exception while setting autocommit off", e)));
  }

}
