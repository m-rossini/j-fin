package uk.co.mr.finance.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;

public record StatementSummary(long count, BigDecimal totalAmount, LocalDate minDate, LocalDate maxDate) {
  private static final Logger LOG = LoggerFactory.getLogger(StatementSummary.class);

  public static final StatementSummary DEFAULT_STATEMENT =
      new StatementSummary(0, BigDecimal.ZERO, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(0));

  public StatementSummary merge(StatementSummary other) {
    LOG.debug("Incoming summary:[{}]", other);
    return new StatementSummary(count + 1,
                                totalAmount.add(other.totalAmount()),
                                minDate.isBefore(other.minDate()) ? minDate : other.minDate(),
                                maxDate.isAfter(other.maxDate()) ? maxDate : other.maxDate());
  }

  public long getCount() {
    return count;
  }

  public BigDecimal totalAmount() {
    return totalAmount;
  }

  public LocalDate minDate() {
    return minDate;
  }

  public LocalDate maxDate() {
    return maxDate;
  }

  public static StatementSummary fromStatement(Statement statement) {
    return new StatementSummary(1,
                                statement.transactionAmount(),
                                statement.transactionDate(),
                                statement.transactionDate());
  }
}
