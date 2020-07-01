package uk.co.mr.finance.domain;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Function;

import static uk.co.mr.finance.Utils.tryBigDecimal;

public record Statement(Integer statementId,
                        Integer transactionOrder,
                        LocalDate transactionDate,
                        String transactionTypeCode,
                        String sortCode,
                        String accountId,
                        String transactionDescription,
                        BigDecimal transactionAmount,
                        BigDecimal totalBalance) {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static Function<String[], Optional<Statement>> transformToStatement() {
        return record -> {
            BigDecimal amount =
                    tryBigDecimal(record[6]).orElse(BigDecimal.ZERO)
                                            .subtract(tryBigDecimal(record[5]).orElse(BigDecimal.ZERO));

            return tryBigDecimal(record[7])
                    .map(balance -> {
                        TransactionType transactionType = new TransactionType(record[1], "");
                        return new Statement(
                                null,
                                null,
                                LocalDate.parse(record[0], formatter),
                                transactionType.transactionTypeCode(),
                                record[2],
                                record[3],
                                record[4],
                                amount,
                                balance
                        );
                    });
        };
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
