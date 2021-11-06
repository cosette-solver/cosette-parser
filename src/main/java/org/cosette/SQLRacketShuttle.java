package org.cosette;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * AN implementation of SqlShuttle interface that could convert a SqlNode instance to a ObjectNode instance.
 */
public class SQLRacketShuttle extends SqlShuttle {

//    private final Environment environment;
    private static ArrayList<String> racketInput;

    /**
     * Initialize the shuttle with a given environment.
     *
     */
    public SQLRacketShuttle() {
        racketInput = new ArrayList<>();
    }

    /**
     * Dump a SqlNode to a writer in JSON format .
     *
     * @param sqlNode The given SQLNode.
     * @param writer   The given writer.
     */
    public static void dumpToRacket(SqlNode sqlNode, Writer writer) throws IOException {
        // writer.write(String);

        SQLRacketShuttle sqlRacketShuttle = new SQLRacketShuttle();

        // Add required headers & modules.
        racketInput.add("#lang rosette\n\n");
        racketInput.add("(require \"../util.rkt\" \"../sql.rkt\" \"../table.rkt\"  \"../evaluator.rkt\" \"../equal.rkt\")\n\n");

        sqlNode.accept(sqlRacketShuttle);

//        System.out.println(sqlNode.toString());
//        System.out.println();
//
//        SqlSelect select = (SqlSelect) sqlNode;
//        System.out.println(select.getGroup());

        // basic implementation
        // need to support SELECT (FROM, WHERE, GROUP BY, HAVING, WHERE), JOIN

        // more advanced
        // need to also support MERGE, ORDER BY, DELETE, DISTINCT (part of SELECT)

        // extra
        // FETCH, LIMIT, ORDER BY (all part of SELECT)

        writer.write(String.join("", racketInput));
    }

    /**
     * @return The String corresponding to the Racket input for the SqlNode instance.
     */
    public String getRacketInput() {
//        return racketInput;
        return "";
    }


    /**
     * Formats a string for the aliased table extracted from a SQL FROM clause.
     * @param from
     * @return String, racket formatted from clause.
     */
    private String helpFormatFrom(String from) {
        String[] fromWords = from.split(" ");
        String toReturn = " ";

        for (String word : fromWords) {
            if (word.equals("AS")) {
                break;
            }
            word = word.replaceAll("`", "");
            toReturn = toReturn + word;
        }
        return toReturn;
    }


    // REQUIRED TO IMPLEMENT INTERFACE

    /**
     * Visits a call to a SqlOperator.
     * @param call
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlCall call) {
        System.out.println("SQL CALL");

        SqlKind sqlKind = call.getKind();
        racketInput.add("(");

        switch (sqlKind) {
            case SELECT:
                System.out.println("\tSQL SELECT\n");
                racketInput.add("SELECT");

                // if there's a group by, string should have SELECT-GROUP
                // otherwise just SELECT
                SqlSelect sqlSelect = (SqlSelect) call;

                List<SqlNode> selectList = sqlSelect.getSelectList();
                if (!selectList.isEmpty()) {
                    racketInput.add(" (VALS");
                }

                for (SqlNode select : selectList) {
                    select.accept(this);
                }
                racketInput.add(")");


                SqlNode from = sqlSelect.getFrom();
                if (from != null) {
                    racketInput.add(" FROM");

                    racketInput.add((" (NAMED"));
                    racketInput.add(helpFormatFrom(from.toString()));
                    racketInput.add(")");
                }

                SqlNode where = sqlSelect.getWhere();
                if (where == null) {
                    racketInput.add(" WHERE (TRUE)");
                } else {
                    racketInput.add(" WHERE");
                    racketInput.add(where.toString());
                }
                break;

            case JOIN:
                System.out.println("\tSQL JOIN");
                break;
        }


        racketInput.add(")");

        System.out.println("racket input");
        System.out.println(String.join("", racketInput));
        return null;
    }

    /**
     * Visits a datatype specification.
     * @param type
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlDataTypeSpec type) {
        System.out.println("type");
        return null;
    }

    /**
     * Visits a dynamic parameter.
     * @param param
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlDynamicParam param) {
        System.out.println("param");
        return null;
    }

    /**
     * Visits an identifier.
     * @param id
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlIdentifier id) {
        System.out.println("ID\n");

        racketInput.add(" \"" + id.toString() + "\"");

        return null;
    }

    /**
     * Visits an interval qualifier.
     * @param intervalQualifier
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        System.out.println("interval qual");
        return null;
    }

    /**
     * Visits a literal.
     * @param literal
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlLiteral literal) {
        System.out.println("literal");
        return null;
    }

    /**
     * Visits a list of SqlNode objects.
     * @param nodeList
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlNodeList nodeList) {
        System.out.println("node list");
        return null;
    }
}
