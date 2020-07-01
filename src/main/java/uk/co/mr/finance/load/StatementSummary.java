package uk.co.mr.finance.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.math.BigDecimal;
import java.time.LocalDate;

public class StatementSummary {
    private static final Logger LOG = LoggerFactory.getLogger(StatementSummary.class);
    private final long count;
    private final BigDecimal totalAmount;
    private final LocalDate minDate;
    private final LocalDate maxDate;

    public static StatementSummary DEFAULT_STATEMENT =
            new StatementSummary(0, BigDecimal.ZERO, LocalDate.MAX, LocalDate.MIN);

    private StatementSummary(long count, BigDecimal totalAmount, LocalDate minDate, LocalDate maxDate) {
        this.count = count;
        this.totalAmount = totalAmount;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

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
