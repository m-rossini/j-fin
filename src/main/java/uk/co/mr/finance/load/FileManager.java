package uk.co.mr.finance.load;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import io.vavr.control.Try;
import org.apache.commons.codec.digest.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FileManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    public boolean canReadFile(Path path) {
        return Files.isReadable(path.toAbsolutePath());
    }

    public Try<String> mayHashFile(Path path) {
        return canReadFile(path.toAbsolutePath())
               ? tryHashFile(path)
               : Try.failure(new IOException(String.format("File [%s] cannot be read", path)));
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

    public <T> Try<Stream<T>> transformFile(Path path, Function<? super String[], Optional<T>> transformer) {
        return Try.of(() -> Files.newBufferedReader(path))
                  .map(reader -> new CSVReaderBuilder(reader)
                          .withSkipLines(1)
                          .withCSVParser(new CSVParserBuilder().build())
                          .build())
                  .map(Iterable::spliterator)
                  .map(iterator -> StreamSupport.stream(iterator, false))
                  .map(records -> records.map(transformer).flatMap(Optional::stream))
                  .onFailure(t -> LOG.warn("Failed to transform file into statements", t));
    }
}
