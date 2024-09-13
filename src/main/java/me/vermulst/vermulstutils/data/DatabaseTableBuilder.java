package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.Main;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class DatabaseTableBuilder {

    private final DatabaseTable databaseTable;

    public DatabaseTableBuilder(String name) {
        this.databaseTable = new DatabaseTable(name);
    }

    protected DatabaseTableBuilder(DatabaseTable databaseTable) {
        this.databaseTable = databaseTable;
    }


    public DatabaseTableBuilder columns(Set<Column> columns) {
        databaseTable.columns = columns;
        return this;
    }

    public DatabaseTable build(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            this.databaseTable.createOrUpdate(statement);
            return this.databaseTable;
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed building database table: " + this.databaseTable.name);
            throw new RuntimeException(e);
        }
    }


}
