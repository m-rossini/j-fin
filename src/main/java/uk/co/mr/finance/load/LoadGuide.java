package uk.co.mr.finance.load;

import io.vavr.control.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.domain.Statement;

import java.nio.file.Path;

public class LoadGuide implements Guide {
    private static final Logger LOG = LoggerFactory.getLogger(LoadGuide.class);

    private DatabaseManager dbManager;

    public LoadGuide(DatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override public Either<Throwable, StatementSummary> guide(Path toLoadPath) {
        StatementLoader statementLoader = new StatementLoader(dbManager, new FileManager());
        return statementLoader.load(toLoadPath, Statement.transformToStatement());
    }
}
