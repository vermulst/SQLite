package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.Main;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

public class Database {

    private final Connection connection;
    private final Set<DatabaseTable> tables = new HashSet<>();

    public Database(String path) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + path);
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed to initiate database");
            throw new RuntimeException(e);
        }
    }

    public void addTable(DatabaseTable table) {
        this.tables.add(table);
    }

    public Connection getConnection() {
        return connection;
    }

    public Set<DatabaseTable> getTables() {
        return tables;
    }
}
