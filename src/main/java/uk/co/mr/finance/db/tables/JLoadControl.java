/*
 * This file is generated by jOOQ.
 */
package uk.co.mr.finance.db.tables;


import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import uk.co.mr.finance.db.Indexes;
import uk.co.mr.finance.db.JPublic;
import uk.co.mr.finance.db.Keys;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class JLoadControl extends TableImpl<Record> {

    private static final long serialVersionUID = 123352533;

    /**
     * The reference instance of <code>public.load_control</code>
     */
    public static final JLoadControl LOAD_CONTROL = new JLoadControl();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<Record> getRecordType() {
        return Record.class;
    }

    /**
     * The column <code>public.load_control.control_id</code>.
     */
    public final TableField<Record, Integer> CONTROL_ID = createField(DSL.name("control_id"), org.jooq.impl.SQLDataType.INTEGER.nullable(false).identity(true), this, "");

    /**
     * The column <code>public.load_control.load_date</code>.
     */
    public final TableField<Record, LocalDate> LOAD_DATE = createField(DSL.name("load_date"), org.jooq.impl.SQLDataType.LOCALDATE.nullable(false), this, "");

    /**
     * The column <code>public.load_control.smallest_date</code>.
     */
    public final TableField<Record, LocalDate> SMALLEST_DATE = createField(DSL.name("smallest_date"), org.jooq.impl.SQLDataType.LOCALDATE, this, "");

    /**
     * The column <code>public.load_control.greatest_date</code>.
     */
    public final TableField<Record, LocalDate> GREATEST_DATE = createField(DSL.name("greatest_date"), org.jooq.impl.SQLDataType.LOCALDATE, this, "");

    /**
     * The column <code>public.load_control.loaded_records</code>.
     */
    public final TableField<Record, Integer> LOADED_RECORDS = createField(DSL.name("loaded_records"), org.jooq.impl.SQLDataType.INTEGER, this, "");

    /**
     * The column <code>public.load_control.load_in_progress</code>.
     */
    public final TableField<Record, Boolean> LOAD_IN_PROGRESS = createField(DSL.name("load_in_progress"), org.jooq.impl.SQLDataType.BOOLEAN.nullable(false), this, "");

    /**
     * The column <code>public.load_control.file_name</code>.
     */
    public final TableField<Record, String> FILE_NAME = createField(DSL.name("file_name"), org.jooq.impl.SQLDataType.VARCHAR(150).nullable(false), this, "");

    /**
     * The column <code>public.load_control.file_hash_code</code>.
     */
    public final TableField<Record, String> FILE_HASH_CODE = createField(DSL.name("file_hash_code"), org.jooq.impl.SQLDataType.VARCHAR(32).nullable(false), this, "");

    /**
     * Create a <code>public.load_control</code> table reference
     */
    public JLoadControl() {
        this(DSL.name("load_control"), null);
    }

    /**
     * Create an aliased <code>public.load_control</code> table reference
     */
    public JLoadControl(String alias) {
        this(DSL.name(alias), LOAD_CONTROL);
    }

    /**
     * Create an aliased <code>public.load_control</code> table reference
     */
    public JLoadControl(Name alias) {
        this(alias, LOAD_CONTROL);
    }

    private JLoadControl(Name alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    private JLoadControl(Name alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    public <O extends Record> JLoadControl(Table<O> child, ForeignKey<O, Record> key) {
        super(child, key, LOAD_CONTROL);
    }

    @Override
    public Schema getSchema() {
        return JPublic.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.<Index>asList(Indexes.IX_LOAD_CONTROL_HASH);
    }

    @Override
    public Identity<Record, Integer> getIdentity() {
        return Keys.IDENTITY_LOAD_CONTROL;
    }

    @Override
    public UniqueKey<Record> getPrimaryKey() {
        return Keys.PK_LOAD_CONTROL_ID;
    }

    @Override
    public List<UniqueKey<Record>> getKeys() {
        return Arrays.<UniqueKey<Record>>asList(Keys.PK_LOAD_CONTROL_ID);
    }

    @Override
    public JLoadControl as(String alias) {
        return new JLoadControl(DSL.name(alias), this);
    }

    @Override
    public JLoadControl as(Name alias) {
        return new JLoadControl(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public JLoadControl rename(String name) {
        return new JLoadControl(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public JLoadControl rename(Name name) {
        return new JLoadControl(name, null);
    }
}
