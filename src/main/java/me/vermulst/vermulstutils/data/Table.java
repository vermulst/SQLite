package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.Main;
import me.vermulst.vermulstutils.data.column.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class Table {

    protected String name;
    protected Set<Column> columns = new HashSet<>();

    protected Table() {
    }

    public TableBuilder builder() {
        return new TableBuilder()
                .name(this.name)
                .columns(columns);
    }

    protected void createOrUpdate(Connection connection) {
        try {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            boolean tableExists = this.tableExists(statement);
            if (!tableExists) {
                this.create(statement);
            } else {
                boolean tableChanged = this.tableChanged(statement);
                if (!tableChanged) return;
                this.update(statement);
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed creating statement for table: " + this.name);
            throw new RuntimeException(e);
        }
    }


    protected void create(Statement statement) {
        try {
            statement.execute(this.getCreateStatement());
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed initiating table: " + this.name);
            throw new RuntimeException(e);
        }
    }

    protected void update(Statement statement) {
        this.name += "_new";
        String newTable = this.getCreateStatement();
        this.name = this.name.substring(0, this.name.length() - 4);
        try {
            this.createNewTable(newTable, statement);
            this.cutOldTable(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tableExists(Statement statement) {
        String checkTableExistsQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + this.name + "';";
        try (ResultSet resultSet = statement.executeQuery(checkTableExistsQuery)) {
            return resultSet.next();
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.SEVERE, "Error checking if table exists: " + this.name, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if the table structure has changed in the database compared to the defined structure.
     */
    protected boolean tableChanged(Statement statement) {
        Set<String> currentColumnNames = new HashSet<>();
        Set<String> currentColumnTypes = new HashSet<>();

        try {
            ResultSet resultSet = statement.executeQuery("PRAGMA table_info(" + this.name + ")");
            while (resultSet.next()) {
                String columnName = resultSet.getString("name");
                String columnType = resultSet.getString("type");
                currentColumnNames.add(columnName);
                currentColumnTypes.add(columnType); // Combine name and type for comparison
            }
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.SEVERE, "Failed to get table info: " + this.name, e);
            throw new RuntimeException(e);
        }

        // Compare column names
        Set<String> definedColumnNames = this.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        // Compare column types
        Set<String> definedColumnTypes = this.columns.stream()
                .map(Column::getColumnTypeName)
                .collect(Collectors.toSet());

        // Compare column names and types to check if any changes occurred
        boolean namesChanged = !currentColumnNames.equals(definedColumnNames);
        boolean typesChanged = !currentColumnTypes.equals(definedColumnTypes);

        return namesChanged || typesChanged;
    }

    private String getCreateStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ").append(this.name).append(" (");
        int totalSize = this.columns.size();
        int count = 0;

        for (Column column : this.columns) {
            count++;
            builder.append(column.getColumnDefinition());
            if (count != totalSize) builder.append(", ");
        }

        Set<String> primaryKeys = this.columns
                .stream().filter(Column::isPrimaryKey)
                .map(Column::getName)
                .collect(Collectors.toUnmodifiableSet());
        if (!primaryKeys.isEmpty()) {
            this.checkValidAutoIncrement(primaryKeys);
            builder.append(", PRIMARY KEY (");
            builder.append(String.join(", ", primaryKeys));
            builder.append(")");
        }

        builder.append(");");
        return builder.toString();
    }

    private void checkValidAutoIncrement(Set<String> primaryKeys) {
        long autoIncrementCount = this.columns
                .stream().filter(column -> column.getColumnProperties().contains(ColumnProperty.AUTO_INCREMENT_PRIMARY_KEY))
                .count();
        if (autoIncrementCount > 0) {
            if (autoIncrementCount > 1) {
                throw new IllegalStateException("Cannot have multiple primary keys with AUTOINCREMENT.");
            }
            if (primaryKeys.size() > 1) {
                throw new IllegalStateException("Cannot have composite primary key with AUTOINCREMENT.");
            }
        }
    }


    private void createNewTable(String newTable, Statement statement) throws SQLException {
        statement.execute(newTable);
    }

    private void cutOldTable(Statement statement) throws SQLException {
        this.copyOldTableIntoNew(statement);
        this.dropTable(statement);
        this.renameNewTable(statement);
    }

    private void copyOldTableIntoNew(Statement statement) throws SQLException {
        Set<String> columnNames = this.getCopyingColumns(statement);
        String columns = String.join(", ", columnNames);
        String copyStatement = "INSERT INTO " + this.name + "_new (" + columns +
                ") SELECT " + columns + " FROM " + this.name + ";";
        statement.execute(copyStatement);
    }

    protected void dropTable(Statement statement) throws SQLException {
        String drop = "DROP TABLE " + this.name + ";";
        statement.execute(drop);
    }

    private void renameNewTable(Statement statement) throws SQLException {
        String renameTableSQL = "ALTER TABLE " + this.name + "_new RENAME TO " + this.name + ";";
        statement.execute(renameTableSQL);
    }

    private Set<String> getCopyingColumns(Statement statement) throws SQLException {
        Set<String> columnNames = this.columns.stream().map(Column::getName).collect(Collectors.toSet());
        ResultSet oldTableColumns = statement.executeQuery("PRAGMA table_info(" + this.name + ")");
        Set<String> oldTableColumnNames = new HashSet<>();
        while (oldTableColumns.next()) {
            oldTableColumnNames.add(oldTableColumns.getString("name"));
        }
        columnNames.removeIf(columnName -> !oldTableColumnNames.contains(columnName));
        return columnNames;
    }
}
