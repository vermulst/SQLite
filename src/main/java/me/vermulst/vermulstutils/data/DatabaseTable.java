package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.Main;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class DatabaseTable {

    protected String name;
    protected Set<Column> columns = new HashSet<>();

    protected DatabaseTable(String name) {
        this.name = name;
    }

    public void createOrUpdate(Statement statement) {
        boolean tableExists = this.tableExists(statement);
        if (!tableExists) {
            this.create(statement);
        } else {
            this.update(statement);
        }
    }

    public void create(Statement statement) {
        try {
            statement.execute(this.getCreateStatement());
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed initiating table: " + this.name);
            throw new RuntimeException(e);
        }
    }

    public void update(Statement statement) {
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

    public DatabaseTableBuilder builder() {
        return new DatabaseTableBuilder(this);
    }

    private String getCreateStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ").append(this.name).append(" (");
        int totalSize = this.columns.size();
        int count = 0;
        for (Column column : this.columns) {
            count++;
            String name = column.getName();
            ColumnType columnType = column.getColumnType();
            builder.append(name).append(" ").append(columnType.getColumnTypeName());
            if (column.isNotNull()) builder.append(" NOT NULL");
            if (count != totalSize) builder.append(", ");
        }
        builder.append(");");
        return builder.toString();
    }

    private void createNewTable(String newTable, Statement statement) throws SQLException {
        statement.execute(newTable);
    }

    private void cutOldTable(Statement statement) throws SQLException {
        this.copyOldTableIntoNew(statement);
        this.dropOldTable(statement);
        this.renameNewTable(statement);
    }

    private void copyOldTableIntoNew(Statement statement) throws SQLException {
        Set<String> columnNames = this.getCopyingColumns(statement);
        String columns = String.join(", ", columnNames);
        String copyStatement = "INSERT INTO " + this.name + "_new (" + columns +
                ") SELECT " + columns + " FROM " + this.name + ";";
        statement.execute(copyStatement);
    }

    private void dropOldTable(Statement statement) throws SQLException {
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
