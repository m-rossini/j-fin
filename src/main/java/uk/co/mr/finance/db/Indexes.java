/*
 * This file is generated by jOOQ.
 */
package uk.co.mr.finance.db;


import org.jooq.Index;
import org.jooq.OrderField;
import org.jooq.impl.Internal;

import uk.co.mr.finance.db.tables.JLoadControl;
import uk.co.mr.finance.db.tables.JStatementData;


/**
 * A class modelling indexes of tables of the <code>public</code> schema.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Indexes {

    // -------------------------------------------------------------------------
    // INDEX definitions
    // -------------------------------------------------------------------------

    public static final Index IX_LOAD_CONTROL_HASH = Indexes0.IX_LOAD_CONTROL_HASH;
    public static final Index IX_STATEMENT_DATA_UNIQUE_1 = Indexes0.IX_STATEMENT_DATA_UNIQUE_1;

    // -------------------------------------------------------------------------
    // [#1459] distribute members to avoid static initialisers > 64kb
    // -------------------------------------------------------------------------

    private static class Indexes0 {
        public static Index IX_LOAD_CONTROL_HASH = Internal.createIndex("ix_load_control_hash", JLoadControl.LOAD_CONTROL, new OrderField[] { JLoadControl.LOAD_CONTROL.FILE_HASH_CODE }, true);
        public static Index IX_STATEMENT_DATA_UNIQUE_1 = Internal.createIndex("ix_statement_data_unique_1", JStatementData.STATEMENT_DATA, new OrderField[] { JStatementData.STATEMENT_DATA.STATEMENT_DATE, JStatementData.STATEMENT_DATA.TRANSACTION_TYPE, JStatementData.STATEMENT_DATA.SORT_CODE, JStatementData.STATEMENT_DATA.ACCOUNT_ID, JStatementData.STATEMENT_DATA.TRANSACTION_DESCRIPTION, JStatementData.STATEMENT_DATA.TRANSACTION_AMOUNT, JStatementData.STATEMENT_DATA.TOTAL_BALANCE }, true);
    }
}
