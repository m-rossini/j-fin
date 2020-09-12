package uk.co.mr.finance.load;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;

public final class UtilForTest {
  public static final String DRIVER_NAME = "org.postgresql.Driver";

  private UtilForTest() {

  }

  public static Path createFile(FileSystem fileSystem,
                                String fileName,
                                String content) throws IOException {
    Path path = fileSystem.getPath("", fileName);

    Files.writeString(path,
                      content,
                      StandardOpenOption.CREATE,
                      StandardOpenOption.DELETE_ON_CLOSE,
                      StandardOpenOption.TRUNCATE_EXISTING,
                      StandardOpenOption.DSYNC);

    return path;
  }

  public static void createDatabase(Connection connection) {
    try {
      Database database =
          DatabaseFactory.getInstance()
                         .findCorrectDatabaseImplementation(new JdbcConnection(connection));
      Liquibase liquibase =
          new Liquibase("db/k_finance_00_master_changelog.xml", new ClassLoaderResourceAccessor(), database);
      liquibase.update("");
    } catch (LiquibaseException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropDatabase(Connection connection) {
    try {
      Database database = DatabaseFactory.getInstance()
                                         .findCorrectDatabaseImplementation(new JdbcConnection(connection));
      Liquibase liquibase =
          new Liquibase("db/k_finance_00_master_changelog.xml", new ClassLoaderResourceAccessor(), database);
      liquibase.dropAll();
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }
}
