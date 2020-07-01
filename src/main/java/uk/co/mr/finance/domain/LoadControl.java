package uk.co.mr.finance.domain;

import java.time.LocalDate;

public record LoadControl(Integer controlId,
                          LocalDate loadDate,
                          LocalDate smallestDate,
                          LocalDate greatestDate,
                          Integer loadedRecords,
                          Boolean loadInProgress,
                          String fileName,
                          String fileHash) {
}
