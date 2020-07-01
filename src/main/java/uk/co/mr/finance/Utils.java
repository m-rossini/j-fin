package uk.co.mr.finance;

import io.vavr.control.Try;

import java.math.BigDecimal;
import java.util.Optional;

public final class Utils {
    private Utils() {
        //left intentionally blank
    }

    public static Optional<BigDecimal> tryBigDecimal(String number) {
        return Try.of(() -> new BigDecimal(number))
                  .toJavaOptional();
    }
}
