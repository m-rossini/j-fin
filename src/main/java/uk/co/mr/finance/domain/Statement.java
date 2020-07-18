package uk.co.mr.finance.domain;

import io.vavr.collection.Seq;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

public record Statement(Integer statementId,
                        Integer transactionOrder,
                        LocalDate transactionDate,
                        String transactionTypeCode,
                        String sortCode,
                        String accountId,
                        String transactionDescription,
                        BigDecimal transactionAmount,
                        BigDecimal totalBalance) {
  private static final Logger LOG = LoggerFactory.getLogger(Statement.class);
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");

  public static Function<String[], Validation<Seq<Throwable>, Statement>> transformToStatement() {
    return record -> {
      Arrays.stream(record).forEach(s -> LOG.debug("Piece:[{}]", s));

      Validation<Throwable, LocalDate> localDateVal =
          Try.of(() -> LocalDate.parse(record[0], formatter)).toValidation();
      TransactionType transactionType = new TransactionType(record[1], "");
      String sortCode = record[2];
      String accountId = record[3];
      String transactionDescription = record[4];

      prepareString(record[5]);
      Validation<Throwable, BigDecimal> crAmountVal =
          Try.of(() -> new BigDecimal(prepareString(record[5]))).toValidation();
      Validation<Throwable, BigDecimal> dbAmountVal =
          Try.of(() -> new BigDecimal(prepareString(record[6]))).toValidation();
      Validation<Throwable, BigDecimal> balanceVal =
          Try.of(() -> new BigDecimal(prepareString(record[7]))).toValidation();

      return Validation.combine(localDateVal, dbAmountVal, crAmountVal, balanceVal)
                       .ap((date, crAmount, dbAmount, balance) ->
                               new Statement(
                                   null,
                                   null,
                                   date,
                                   transactionType.transactionTypeCode(),
                                   sortCode,
                                   accountId,
                                   transactionDescription,
                                   crAmount.subtract(dbAmount),
                                   balance
                               ));

    };
  }

  private static String prepareString(String value) {
    return Optional.ofNullable(value).map(String::strip).filter(s -> !s.isBlank()).orElse("0");
  }

  public Statement withStatementId(Integer id) {
    return new Statement(id,
                         transactionOrder,
                         transactionDate,
                         transactionTypeCode,
                         sortCode,
                         accountId,
                         transactionDescription,
                         transactionAmount,
                         totalBalance);
  }
}
