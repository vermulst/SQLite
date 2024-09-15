package me.vermulst.vermulstutils.data;


import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Database {

    private String path;
    private Connection connection;
    private final Set<Table<?>> tables = new HashSet<>();

    protected Database() {
    }

    protected void path(String path) {
        this.path = path;
    }

    protected void tables(Set<Table.Builder> tableBuilders) {
        try (Connection connection = this.getConnection()) {
            for (Table.Builder builder : tableBuilders) {
                Table table = builder.build(connection);
                this.addTable(table, connection);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateTable(Table table) {
        try (Connection connection = this.getConnection()) {
            table.createOrUpdate(connection);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addTable(Table<?> table) {
        try (Connection connection = this.getConnection()) {
            this.addTable(table, connection);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addTable(Table<?> table, Connection connection) {
        Set<String> tableNames = this.getTableNames();
        if (tableNames.contains(table.getName())) {
            this.tables.removeIf(table1 -> table1.getName().equalsIgnoreCase(table.getName()));
        }
        this.tables.add(table);
        table.createOrUpdate(connection);
    }

    public <PK> Table<PK> getTable(Class<PK> tableType, String name) {
        Optional<Table<?>> tableOptional = this.tables.stream()
                .filter(table -> name.equalsIgnoreCase(table.getName()))
                .findFirst();
        // Perform type checking or casting based on tableType
        if (tableOptional.isPresent() && tableType.isInstance(tableOptional.get())) {
            return (Table<PK>) tableType.cast(tableOptional.get());
        }
        return null;
    }


    public void dropTable(Table<?> table) {
        this.dropTable(table.name);
    }

    public void dropTable(String name) {
        try (Connection connection = this.getConnection()) {
            this.dropTable(name, connection);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop table: " + name + " (might not exist)", e);
        }
    }

    public void dropTable(String name, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String drop = "DROP TABLE " + name + ";";
        statement.execute(drop);
        statement.close();
        connection.commit();
    }

    public void dropAllTables() {
        try (Connection connection = this.getConnection()) {
            for (Table<?> table : this.tables) {
                this.dropTable(table.name, connection);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop all tables", e);
        }
    }

    public void closeConnection() {
        try {
            if (this.connection == null || this.connection.isClosed()) return;
            //this.connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete() {
        this.closeConnection();
        this.dropAllTables();
        File file = new File(this.path);
        if (file.exists()) {
            if (file.delete()) {
                System.out.println("Database file deleted successfully.");
            } else {
                System.out.println("Failed to delete the database file. (no permission)");
            }
        } else {
            System.out.println("Failed deleting database, (file does not exist).");
        }
    }

    public Connection getConnection() {
        try {
            if (this.connection != null && !this.connection.isClosed()) return this.connection;
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            this.connection.setAutoCommit(false);
            return this.connection;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to database with path " + path, e);
        }
    }


    public Set<Table<?>> getTables() {
        return tables;
    }

    private Set<String> getTableNames() {
        return this.tables.stream().map(Table::getName).collect(Collectors.toUnmodifiableSet());
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String path;
        private Set<Table.Builder> builders = new HashSet<>();
        private boolean addExistingTables = false;

        private Builder() {

        }

        public Builder path(String path) {
            if (!path.endsWith(".db")) path += ".db";
            this.path = path;
            return this;
        }

        public Builder addTablesIfNotExists(Table.Builder... tableBuilders) {
            this.builders = Set.of(tableBuilders);
            return this;
        }

        public Builder addExistingTables() {
            addExistingTables = true;
            return this;
        }

        private void addExistingTables(Database database) {
            try (Connection connection = database.getConnection()) {
                List<String> tableNames = new ArrayList<>();
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet rs = metaData.getTables(null, null, "%", new String[] { "TABLE" });
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tableNames.add(tableName);
                }
                rs.close();
                for (String tableName : tableNames) {
                    Class<?> tableClass = this.determinePrimaryKeyClass(connection, tableName);
                    Table<?> table = Table.builder(tableClass)
                            .name(tableName)
                            .findColumns(connection)
                            .build(connection);
                    database.getTables().add(table);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed adding existing tables", e);
            }
        }

        private Class<?> determinePrimaryKeyClass(Connection connection, String tableName) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getPrimaryKeys(null, null, tableName);
            List<Class<?>> primaryKeyTypes = new ArrayList<>();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                ResultSet columnRs = metaData.getColumns(null, null, tableName, columnName);
                if (columnRs.next()) {
                    int sqlType = columnRs.getInt("DATA_TYPE");
                    Class<?> javaType = Column.REVERSED_TYPE_MAP.getOrDefault(sqlType, Object.class); // Map SQL type to Java type
                    primaryKeyTypes.add(javaType);
                }
                columnRs.close();
            }
            rs.close();
            if (primaryKeyTypes.size() == 1) {
                return primaryKeyTypes.get(0); // Single primary key, return the type
            } else if (primaryKeyTypes.size() > 1) {
                return CompositeKey.class; // Composite key, return CompositeKey class
            } else {
                throw new SQLException("No primary key found for table: " + tableName);
            }
        }


        public Database build() {
            Database database = new Database();
            database.path(this.path);
            database.closeConnection();
            if (addExistingTables) this.addExistingTables(database);
            database.tables(this.builders);
            return database;
        }

    }

}
