package org.cosette;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.ServerDdlExecutor;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private final CalciteConnection calciteConnection;

    /**
     * Create a SchemaGenerator instance by setting up a connection to JDBC.
     */
    public SchemaGenerator() throws SQLException {
        Properties info = new Properties();
        info.setProperty(CalciteConnectionProperty.LEX.camelName(), "mysql");
        info.setProperty(CalciteConnectionProperty.FUN.camelName(), "standard");
        info.setProperty(CalciteConnectionProperty.QUOTING.camelName(), "double_quote");
        info.setProperty(CalciteConnectionProperty.FORCE_DECORRELATE.camelName(), "false");
        info.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), "false");
        info.setProperty(CalciteConnectionProperty.PARSER_FACTORY.camelName(), ServerDdlExecutor.class.getName() + "#PARSER_FACTORY");
        calciteConnection = DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
    }

    /**
     * Execute a DML statement.
     *
     * @param dml The given DML statement.
     */
    public void applyDML(String dml) throws SQLException {
        Statement statement = calciteConnection.createStatement();
        statement.executeUpdate(dml);
        statement.close();
    }

    /**
     * @return The current schema.
     */
    public SchemaPlus extractSchema() {
        return calciteConnection.getRootSchema();
    }

    /**
     * @return A RawPlanner instance based on the extracted schema.
     */
    public RawPlanner createPlanner() {
        return new RawPlanner(extractSchema());
    }

}
