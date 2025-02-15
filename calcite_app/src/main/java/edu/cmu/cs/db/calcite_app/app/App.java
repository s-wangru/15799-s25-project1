package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import edu.cmu.cs.db.calcite_app.app.App.CustomSchema;
import edu.cmu.cs.db.calcite_app.app.App.JdbcScannableTable;
import edu.cmu.cs.db.calcite_app.app.App.ListEnumerator;



public class App
{

    public static class ListEnumerator implements Enumerator<Object[]> {
        private final Iterator<Object[]> iterator;
        private Object[] current;

        public ListEnumerator(List<Object[]> data) {
            this.iterator = data.iterator();
        }

        @Override
        public Object[] current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            return false;
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
    // public static class ResultSetEnumerator implements Enumerator<Object[]> {
    //     private final ResultSet resultSet;
    //     private final int columnCount;
    //     private final Connection connection;
    //     private final Statement statement;
    //     private final String tablename;

    //     public ResultSetEnumerator(ResultSet resultSet, Statement statement, Connection connection, String tablename) throws SQLException {
    //         this.resultSet = resultSet;
    //         this.columnCount = resultSet.getMetaData().getColumnCount();
    //         this.connection = connection;
    //         this.statement = statement;
    //         this.tablename = tablename;
    //     }

    //     @Override
    //     public Object[] current() {
    //         try {
    //             Object[] row = new Object[columnCount];
    //             ResultSetMetaData metaData = resultSet.getMetaData();
    //             for (int i = 0; i < columnCount; i++) {
    //                 if (metaData.getColumnTypeName(i+1).equals("DATE")){
    //                     row[i] = resultSet.getDate(i + 1);
    //                 } else {
    //                     row[i] = resultSet.getObject(i + 1);
    //                 }
    //             }
    //             return row;
    //         } catch (SQLException e) {
    //             throw new RuntimeException("Error fetching row", e);
    //         }
    //     }

    //     @Override
    //     public boolean moveNext() {
    //         try {
    //             return resultSet.next();
    //         } catch (SQLException e) {
    //             throw new RuntimeException("Error moving to next row", e);
    //         }
    //     }

    //     @Override
    //     public void reset() {
    //         throw new UnsupportedOperationException();
    //     }

    //     @Override
    //     public void close() {
    //         try {
    //             System.out.println("Closing result set for " + tablename);
    //             resultSet.close();
    //             statement.close();
    //             connection.close();
    //         } catch (SQLException e) {
    //             throw new RuntimeException("Error closing resources", e);
    //         }
    //     }
    // }
    
    public static class JdbcScannableTable extends AbstractTable implements ScannableTable {
        private final String tableName;
        private final List<ColumnDef> columns;
        private final List<Object[]> cachedData; 

        public JdbcScannableTable(String tableName, List<ColumnDef> columns, List<Object[]> cachedData) {
            this.tableName = tableName;
            this.columns = columns;
            this.cachedData = cachedData;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (ColumnDef col : columns) {
                RelDataType colType = col.getType(typeFactory);
                colType = typeFactory.createTypeWithNullability(colType, col.isNullable());
                builder.add(col.getName(), colType);
            }
            return builder.build();
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            System.out.println("Scanning table (cached) " + tableName);
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new ListEnumerator(cachedData); 
                }
            };
        }

        @Override
        public String toString() {
            return "JdbcScannableTable[" + tableName + "]";
        }
    
        /**
         * A helper class to define a column's name, type, and nullability.
         */
        public static class ColumnDef {
            private final String name;
            private final String type;   // e.g., "BIGINT", "VARCHAR", "DECIMAL(15,2)", "DATE"
            private final boolean isNullable;
    
            public ColumnDef(String name, String type, boolean isNullable) {
                this.name = name;
                this.type = type;
                this.isNullable = isNullable;
            }
    
            public String getName() {
                return name;
            }
    
            /**
             * Converts the type string into a Calcite RelDataType.
             */
            public RelDataType getType(RelDataTypeFactory typeFactory) {
                if (type.equalsIgnoreCase("BIGINT")) {
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                } else if (type.equalsIgnoreCase("INTEGER")) {
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                } else if (type.equalsIgnoreCase("VARCHAR")) {
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
                } else if (type.startsWith("DECIMAL")) {
                    // Expecting format like "DECIMAL(15,2)"
                    int start = type.indexOf('(');
                    int end = type.indexOf(')');
                    if (start > 0 && end > start) {
                        String params = type.substring(start + 1, end);
                        String[] parts = params.split(",");
                        int precision = Integer.parseInt(parts[0].trim());
                        int scale = Integer.parseInt(parts[1].trim());
                        if (typeFactory instanceof org.apache.calcite.jdbc.JavaTypeFactoryImpl) {
                            return ((org.apache.calcite.jdbc.JavaTypeFactoryImpl) typeFactory)
                                    .createSqlType(SqlTypeName.DECIMAL, precision, scale);
                        } else {
                            return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision);
                        }
                    } else {
                        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
                    }
                } else if (type.equalsIgnoreCase("DATE")) {
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                } else {
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
                }
            }
    
            public boolean isNullable() {
                return isNullable;
            }
        }
    }

    public static class CustomSchema extends AbstractSchema {
        private final Map<String, Table> tableMap = new HashMap<>();
        public CustomSchema(File parquetFolder) {
            // Register "customer" table.
            tableMap.put("customer", loadTable("customer", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("c_custkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("c_name", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("c_address", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("c_nationkey", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("c_phone", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("c_acctbal", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("c_mktsegment", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("c_comment", "VARCHAR", false)
            )));

            // Register "lineitem" table.
            tableMap.put("lineitem", loadTable("lineitem", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("l_orderkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("l_partkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("l_suppkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("l_linenumber", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("l_quantity", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("l_extendedprice", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("l_discount", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("l_tax", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("l_returnflag", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("l_linestatus", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("l_shipdate", "DATE", false),
                new JdbcScannableTable.ColumnDef("l_commitdate", "DATE", false),
                new JdbcScannableTable.ColumnDef("l_receiptdate", "DATE", false),
                new JdbcScannableTable.ColumnDef("l_shipinstruct", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("l_shipmode", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("l_comment", "VARCHAR", false)
            )));

            // Register "nation" table.
            tableMap.put("nation", loadTable("nation", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("n_nationkey", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("n_name", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("n_regionkey", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("n_comment", "VARCHAR", false)
            )));

            // Register "orders" table.
            tableMap.put("orders", loadTable("orders", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("o_orderkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("o_custkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("o_orderstatus", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("o_totalprice", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("o_orderdate", "DATE", false),
                new JdbcScannableTable.ColumnDef("o_orderpriority", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("o_clerk", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("o_shippriority", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("o_comment", "VARCHAR", false)
            )));

            // Register "part" table.
            tableMap.put("part", loadTable("part", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("p_partkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("p_name", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("p_mfgr", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("p_brand", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("p_type", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("p_size", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("p_container", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("p_retailprice", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("p_comment", "VARCHAR", false)
            )));

            // Register "partsupp" table.
            tableMap.put("partsupp", loadTable("partsupp", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("ps_partkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("ps_suppkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("ps_availqty", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("ps_supplycost", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("ps_comment", "VARCHAR", false)
            )));

            // Register "region" table.
            tableMap.put("region", loadTable("region", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("r_regionkey", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("r_name", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("r_comment", "VARCHAR", false)
            )));

            // Register "supplier" table.
            tableMap.put("supplier", loadTable("supplier", parquetFolder, List.of(
                new JdbcScannableTable.ColumnDef("s_suppkey", "BIGINT", false),
                new JdbcScannableTable.ColumnDef("s_name", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("s_address", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("s_nationkey", "INTEGER", false),
                new JdbcScannableTable.ColumnDef("s_phone", "VARCHAR", false),
                new JdbcScannableTable.ColumnDef("s_acctbal", "DECIMAL(15,2)", false),
                new JdbcScannableTable.ColumnDef("s_comment", "VARCHAR", false)
            )));

            defineView("revenue0r15721",
                "SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue " +
                "FROM read_parquet('" + new File(parquetFolder, "lineitem.parquet").getAbsolutePath() + "') " +
                "WHERE l_shipdate >= DATE '1995-01-01' " +
                "AND l_shipdate < DATE '1995-04-01' " +
                "GROUP BY l_suppkey"
            );
        }

        private void defineView(String viewName, String query) {
            try (Connection duckdbConnection = DriverManager.getConnection("jdbc:duckdb:");
                 Statement stmt = duckdbConnection.createStatement()) {
                stmt.execute("CREATE VIEW " + viewName + " AS " + query);
                System.out.println("Created view: " + viewName);
            } catch (SQLException e) {
                throw new RuntimeException("Error defining view " + viewName, e);
            }
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return tableMap;
        }

        /**
         * Optionally, you can add a method to register an additional table.
         *
         * @param tableName The table name.
         * @param table     The Table instance.
         */
        public void addTable(String tableName, Table table) {
            tableMap.put(tableName, table);
        }

        private JdbcScannableTable loadTable(String tableName, File folder, List<JdbcScannableTable.ColumnDef> columns) {
            List<Object[]> tableData = new ArrayList<>();
            File parquetFile = new File(folder, tableName + ".parquet");
        

            String duckUrl = "jdbc:duckdb:"; // In-memory DuckDB instance.
            String query = "SELECT * FROM read_parquet('" + parquetFile.getAbsolutePath().replace("\\", "\\\\") + "')";
            System.out.println("Executing query: " + query);
            try {
                Connection duckConnection = DriverManager.getConnection(duckUrl);

                Statement statement = duckConnection.createStatement();
                statement.execute("SET memory_limit = '2GB'");
                ResultSet resultSet = statement.executeQuery(query);

                int columnCount = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = resultSet.getObject(i + 1);
                    }
                    tableData.add(row);
                }
                System.out.println("Loaded " + tableData.size() + " rows from " + tableName + " (Parquet).");
            } catch (SQLException e) {
                throw new RuntimeException("Error loading table " + tableName + " from Parquet file: " + parquetFile.getAbsolutePath(), e);
            }
            return new JdbcScannableTable(tableName, columns, tableData);
        }
    }

    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(), RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(", ");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(", ");
                }
                resultSetString.append(resultSet.getString(i));
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    private static String convertRelNodeToSql(RelNode relNode) {
        RelToSqlConverter converter = new RelToSqlConverter(RedshiftSqlDialect.DEFAULT);
        SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
        return sqlNode.toSqlString(RedshiftSqlDialect.DEFAULT).getSql();
    }

    private static Planner getPlanner(CalciteConnection calciteConnection) {
        SqlParser.Config parserConfig = SqlParser.Config.DEFAULT.withCaseSensitive(false);
        SchemaPlus defaultSchema = calciteConnection.getRootSchema().getSubSchema("MY_SCHEMA");
        FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(parserConfig)
            .defaultSchema(defaultSchema)
            .build();
        return Frameworks.getPlanner(config);
    }

    private static RelNode optimizeRelNode(RelNode relNode) {
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        programBuilder.addRuleInstance(CoreRules.FILTER_MERGE);
        // programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
        HepProgram hepProgram = programBuilder.build();

        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(relNode);
        return hepPlanner.findBestExp();
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);
        String parquetFolderPath = args[2];
        System.out.println("\tArg3: " + parquetFolderPath);
        
        // Note: in practice, you would probably use org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure Calcite.
        // But there's a lot of magic happening there; since this is an educational project,
        // we guide you towards the explicit method in the writeup.

        Properties info = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Set up a simple default schema.
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Class.forName("org.duckdb.DuckDBDriver");
        // DataSource ds = JdbcSchema.dataSource("jdbc:duckdb:/Users/ruiqiwang/Desktop/15799-s25-project1/input/test.db", null, null, null);

        rootSchema.add("MY_SCHEMA", new CustomSchema(new File(parquetFolderPath)));

        calciteConnection.setSchema("MY_SCHEMA");
        System.out.println(rootSchema.getSubSchema("MY_SCHEMA").getTableNames());

        // List all .sql files in the input queries directory.
        File inputDir = new File(arg1);
        File[] sqlFiles = inputDir.listFiles((dir, name) -> name.endsWith(".sql"));
        if (sqlFiles == null || sqlFiles.length == 0) {
            System.err.println("No SQL files found in directory: " + arg1);
            return;
        }

        for (File sqlFile : sqlFiles) {
            // Derive a base name (e.g., "foo" from "foo.sql").
            String fileName = sqlFile.getName();
            String baseName = fileName.substring(0, fileName.lastIndexOf(".sql"));

            // Read the original SQL query.
            String sql = Files.readString(sqlFile.toPath());

            // Write the original SQL to output_dir/<baseName>.sql.
            File origSqlOut = new File(arg2, baseName + ".sql");
            Files.writeString(origSqlOut.toPath(), sql);

            // Create a Planner (using the connection's schema).
            Planner planner = getPlanner(calciteConnection);
            // Parse the SQL.
            SqlNode parsed = planner.parse(sql);
            // Validate the parsed SQL.
            System.out.println('\n' + parsed.toString());
            SqlNode validated = planner.validate(parsed);
            // Convert the validated SQL into a relational expression.
            RelNode originalRel = planner.rel(validated).project();
            
            // Dump the original (unoptimized) plan to output_dir/<baseName>.txt.
            File initialPlanOut = new File(arg2, baseName + ".txt");
            SerializePlan(originalRel, initialPlanOut);

            // Optimize the plan (apply heuristic and/or cost-based rules).
            RelNode optimizedRel = optimizeRelNode(originalRel);
            System.out.println("optimized plan");
            // Dump the optimized plan to output_dir/<baseName>_optimized.txt.
            File optimizedPlanOut = new File(arg2, baseName + "_optimized.txt");
            SerializePlan(optimizedRel, optimizedPlanOut);


            // Convert the optimized plan back into SQL.
            String optimizedSql = convertRelNodeToSql(optimizedRel);

            // Write the deparsed SQL to output_dir/<baseName>_optimized.sql.
            File optimizedSqlOut = new File(arg2, baseName + "_optimized.sql");
            Files.writeString(optimizedSqlOut.toPath(), optimizedSql);
            System.out.println("converted optimized plan to sql");

            // Execute the optimized plan using a RelRunner.
            ResultSet resultSet = calciteConnection.createStatement().executeQuery(optimizedSql);
            File resultsCsv = new File(arg2, baseName + "_results.csv");
            SerializeResultSet(resultSet, resultsCsv);
            

            System.out.println("Processed query: " + baseName);
        }
        
        // Close the connection.
        connection.close();
    }
}
