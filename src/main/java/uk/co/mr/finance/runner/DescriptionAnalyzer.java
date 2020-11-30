package uk.co.mr.finance.runner;

import io.vavr.control.Try;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.mr.finance.load.DatabaseManager;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static uk.co.mr.finance.db.Tables.STATEMENT_DATA;

public class DescriptionAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(DescriptionAnalyzer.class);

  private final String userId = "finance";
  private final String userPwd = "finance";
  private final String connectionString = "jdbc:postgresql://localhost:5432/finance";
  private final String driverName = "org.postgresql.Driver";

  //TODO INJECTION
  private final Pattern amazonPattern =
      Pattern.compile("(AMZN|AMAZON\\.CO)(\\s*)(MKTP|DIGITAL)?(\\s?)(\\.?)(UK)?[a-zA-Z0-9\\s*]*", Pattern.CASE_INSENSITIVE);
  private final Pattern fiveGuysPattern = Pattern.compile("FIVE GUYS\\s(.*)", Pattern.CASE_INSENSITIVE);
  private final Pattern kfcPattern = Pattern.compile("KFC([\\s\\-])(.*)", Pattern.CASE_INSENSITIVE);
  private final Pattern lidlPattern = Pattern.compile("LIDL([\\s\\-])(.*)", Pattern.CASE_INSENSITIVE);
  private final Pattern moneyWithdrawalPattern = Pattern.compile("LNK(\\s+)([^\\s]+)(\\s+)([^\\s]+)", Pattern.CASE_INSENSITIVE);
  private final Pattern sainsburyPattern = Pattern.compile("([SAINSBURY]+(')?[S])\\b(\\s*\\w*)*", Pattern.CASE_INSENSITIVE);

  private final List<Pattern> patternList =
      List.of(amazonPattern, fiveGuysPattern, kfcPattern, lidlPattern, moneyWithdrawalPattern, sainsburyPattern);

  private final Map<String, Set<String>> mappedDescriptions =
      patternList.stream().map(Pattern::pattern).collect(Collectors.toMap(Function.identity(), k -> new HashSet<>()));

  private final Set<String> unmatched = new HashSet<>();

  public static void main(String[] args) {
    DescriptionAnalyzer analyzer = new DescriptionAnalyzer();
    analyzer.process();
  }

  private void process() {
    DatabaseManager dbManager = DatabaseManager.from(driverName, connectionString, userId, userPwd);
    Try<Connection> tryConnection = dbManager.getConnection();
    DSLContext context = tryConnection.map(c -> DSL.using(c, SQLDialect.POSTGRES)).getOrElseThrow(() -> new IllegalArgumentException());

    try (Cursor<Record> cursor = context.selectFrom(STATEMENT_DATA).fetchLazy();) {
      while (cursor.hasNext()) {
        Record record = cursor.fetchNext();
        String desc = record.get(STATEMENT_DATA.TRANSACTION_DESCRIPTION);

        boolean match = false;
        for (Pattern pattern : patternList) {
          Matcher matcher = pattern.matcher(desc);
          if (matcher.matches()) {
            Set<String> merge = mappedDescriptions.merge(pattern.pattern(), new HashSet<>(), (l1, l2) -> l1);
            merge.add(desc);
            match = true;
          }
          if (!match) {
            unmatched.add(desc);
          }
        }
      }
      //TODO Check here if one desc is a more than one MappedDescription Value List
    }

    context.close();

    tryConnection.peek(DatabaseManager::safeCloseConnection);
    dbManager.safeClose();
  }
}
