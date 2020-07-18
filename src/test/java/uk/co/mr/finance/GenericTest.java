package uk.co.mr.finance;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GenericTest {
  private static final Logger LOG = LoggerFactory.getLogger(GenericTest.class);

  private static Path path;

  @BeforeAll
  public static void create_file_system_and_file() {
    FileSystem fileSystem = Jimfs.newFileSystem(Configuration.windows());
    path = fileSystem.getPath("", "text.txt");
  }

  @BeforeEach
  public void create_file() throws IOException {
    String content = """
        line1
        line2
        line3
        """;
    Files.writeString(path,
                      content,
                      StandardOpenOption.CREATE,
                      StandardOpenOption.DSYNC);

  }

  @Test
  public void nested() {
    try {
      FileInputStream inputStream = new FileInputStream(new File(""));
      try {
        DriverManager.getConnection("");
      } catch (SQLException e) {
        e.printStackTrace();
            

      } finally {
        System.out.println();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    ;
  }

  @Test
  public void test1() throws IOException {
    Files.lines(path).forEach(line -> LOG.info("{}", line));
  }

  @AfterEach
  public void delete_file() throws IOException {
    Path path = Paths.get("a", "b");
    Path resolve = path.resolve("out.txt");
    Files.deleteIfExists(GenericTest.path);
  }

}
