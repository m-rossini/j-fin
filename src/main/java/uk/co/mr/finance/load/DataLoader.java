package uk.co.mr.finance.load;

import io.vavr.Tuple2;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import uk.co.mr.finance.domain.Statement;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

public interface DataLoader<T, R> {
    Tuple2<Optional<Throwable>, Optional<StatementSummary>> load(Path path, Function<String[], Validation<Seq<Throwable>, Statement>> transformer);
}
