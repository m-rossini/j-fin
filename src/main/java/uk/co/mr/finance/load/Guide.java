package uk.co.mr.finance.load;

import io.vavr.control.Either;

import java.nio.file.Path;

public interface Guide {

    Either<Throwable, StatementSummary> guide(Path toLoadPath);
}
