package uk.co.mr.finance.load;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import io.vavr.collection.Seq;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import org.apache.commons.codec.digest.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class InputDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(InputDataManager.class);

  public boolean canReadFile(Path path) {
    return Files.isReadable(path.toAbsolutePath());
  }

  public Try<String> mayHashFile(Path path) {
    return canReadFile(path.toAbsolutePath())
           ? tryHashFile(path)
           : Try.failure(new IOException(String.format("File [%s] cannot be read", path.toAbsolutePath())));
  }

  public Try<String> tryHashFile(Path path) {
    return Try.of(() -> hashFile(path))
              .map(longs -> Long.toHexString(longs[0]) + Long.toHexString(longs[1]));
  }

  private long[] hashFile(Path path) throws IOException {
    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
    ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
    int read = channel.read(buffer);
    return MurmurHash3.hash128x64(buffer.array());
  }

  public Try<Stream<Validation<Seq<Throwable>, Statement>>> transformFile(Path path, Function<String[], Validation<Seq<Throwable>, Statement>> transformer) {
    return Try.of(() -> Files.newBufferedReader(path))
              .map(reader -> new CSVReaderBuilder(reader)
                  .withSkipLines(1)
                  .withCSVParser(new CSVParserBuilder().build())
                  .build())
              .map(Iterable::spliterator)
              .map(iterator -> StreamSupport.stream(iterator, false))
              .map(records -> records.map(transformer))
              .onSuccess(s -> LOG.info("Mapped statements stream from file were generated"))
              .onFailure(t -> LOG.warn("Failed to transform file into statements", t));
  }
}
