package uk.co.mr.finance.load;

import io.vavr.control.Either;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

public interface DataLoader<T, R> {
    Either<Throwable, StatementSummary> load(Path path, Function<? super String[], Optional<T>> transformer);
}
