/*
 * This file is generated by jOOQ.
 */
package uk.co.mr.finance.db;


import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.Internal;

import uk.co.mr.finance.db.tables.JDatabasechangeloglock;
import uk.co.mr.finance.db.tables.JLoadControl;
import uk.co.mr.finance.db.tables.JStatementData;
import uk.co.mr.finance.db.tables.JTransactionType;


/**
 * A class modelling foreign key relationships and constraints of tables of 
 * the <code>public</code> schema.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // IDENTITY definitions
    // -------------------------------------------------------------------------

    public static final Identity<Record, Integer> IDENTITY_LOAD_CONTROL = Identities0.IDENTITY_LOAD_CONTROL;
    public static final Identity<Record, Integer> IDENTITY_STATEMENT_DATA = Identities0.IDENTITY_STATEMENT_DATA;

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<Record> DATABASECHANGELOGLOCK_PKEY = UniqueKeys0.DATABASECHANGELOGLOCK_PKEY;
    public static final UniqueKey<Record> PK_LOAD_CONTROL_ID = UniqueKeys0.PK_LOAD_CONTROL_ID;
    public static final UniqueKey<Record> PK_STATEMENT_DATA_ID = UniqueKeys0.PK_STATEMENT_DATA_ID;
    public static final UniqueKey<Record> PK_TRANSACTION__TYPE = UniqueKeys0.PK_TRANSACTION__TYPE;

    // -------------------------------------------------------------------------
    // FOREIGN KEY definitions
    // -------------------------------------------------------------------------

    public static final ForeignKey<Record, Record> STATEMENT_DATA__FK_TRANSACTION_TYPE = ForeignKeys0.STATEMENT_DATA__FK_TRANSACTION_TYPE;

    // -------------------------------------------------------------------------
    // [#1459] distribute members to avoid static initialisers > 64kb
    // -------------------------------------------------------------------------

    private static class Identities0 {
        public static Identity<Record, Integer> IDENTITY_LOAD_CONTROL = Internal.createIdentity(JLoadControl.LOAD_CONTROL, JLoadControl.LOAD_CONTROL.CONTROL_ID);
        public static Identity<Record, Integer> IDENTITY_STATEMENT_DATA = Internal.createIdentity(JStatementData.STATEMENT_DATA, JStatementData.STATEMENT_DATA.STATEMENT_ID);
    }

    private static class UniqueKeys0 {
        public static final UniqueKey<Record> DATABASECHANGELOGLOCK_PKEY = Internal.createUniqueKey(JDatabasechangeloglock.DATABASECHANGELOGLOCK, "databasechangeloglock_pkey", new TableField[] { JDatabasechangeloglock.DATABASECHANGELOGLOCK.ID }, true);
        public static final UniqueKey<Record> PK_LOAD_CONTROL_ID = Internal.createUniqueKey(JLoadControl.LOAD_CONTROL, "pk_load_control_id", new TableField[] { JLoadControl.LOAD_CONTROL.CONTROL_ID }, true);
        public static final UniqueKey<Record> PK_STATEMENT_DATA_ID = Internal.createUniqueKey(JStatementData.STATEMENT_DATA, "pk_statement_data_id", new TableField[] { JStatementData.STATEMENT_DATA.STATEMENT_ID }, true);
        public static final UniqueKey<Record> PK_TRANSACTION__TYPE = Internal.createUniqueKey(JTransactionType.TRANSACTION_TYPE, "pk_transaction__type", new TableField[] { JTransactionType.TRANSACTION_TYPE.TRANSACTION_TYPE_ }, true);
    }

    private static class ForeignKeys0 {
        public static final ForeignKey<Record, Record> STATEMENT_DATA__FK_TRANSACTION_TYPE = Internal.createForeignKey(Keys.PK_TRANSACTION__TYPE, JStatementData.STATEMENT_DATA, "fk_transaction_type", new TableField[] { JStatementData.STATEMENT_DATA.TRANSACTION_TYPE }, true);
    }
}
