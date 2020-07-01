package uk.co.mr.finance.load;

import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Savepoint;

public class DatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);

    private final String driverName;
    private final String connectString;
    private final String userId;
    private final String userPwd;
    private final Try<Connection> connection;

    public DatabaseManager(String driverName, String connectString, String userId, String userPwd) {
        this.driverName = driverName;
        this.connectString = connectString;
        this.userId = userId;
        this.userPwd = userPwd;

        this.connection = connect();

    }

    public DatabaseManager(Connection connection) {
        this.driverName = null;
        this.connectString = null;
        this.userId = null;
        this.userPwd = null;

        this.connection = Try.of(() -> connection);
    }

    private Try<Connection> connect() {
        return loadDriver(driverName).toTry().flatMap(v -> safeConnect(connectString, userId, userPwd));
    }

    private Option<? extends Class<?>> loadDriver(String driverName) {
        return Try.of(() -> Class.forName(driverName))
                  .onFailure(e -> LOG.error("Unable to load Driver", e))
                  .toOption();
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
