package liquibase.database.core;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

public class VoltDBDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "VoltDB";
    
    public VoltDBDatabase() {
        super.setCurrentDateTimeFunction("CURRENT_TIMESTAMP");
    }
    
    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);
    }

    /**
     * Always returns null or DATABASECHANGELOG table may not be found.
     */
    @Override
    public String getDefaultCatalogName() {
        return null;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:voltdb")) {
            return "com.voltdb.jdbc.Driver";
        }
        return null;
    }

    @Override
    public String getShortName() {
        return "voltdb";
    }

    @Override
    public Integer getDefaultPort() {
        return 21212;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public boolean getAutoCommitMode() {
        return false;
    }

    @Override
    public void setAutoCommit(final boolean b) throws DatabaseException {
        // Feature not supported VoltDB is always in auto commit
    }
    
    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return currentDateTimeFunction;
    }

    private String findCurrentDateTimeFunction() {
        return "NOW";
    }

    @Override
    public String getLineComment() {
        return "-- ";
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    public boolean supportsBooleanDataType() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }
}
