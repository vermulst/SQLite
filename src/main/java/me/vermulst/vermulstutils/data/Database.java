package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.Main;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

public class Database {

    private Connection connection;
    private final Set<Table> tables = new HashSet<>();

    protected Database() {
    }

    protected void initiateConnection(String path) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + path);
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed to initiate database");
            throw new RuntimeException(e);
        }
    }

    protected void tables(Set<TableBuilder> tableBuilders) {
        for (TableBuilder builder : tableBuilders) {
            Table table = builder.build(this.connection());
            this.addTable(table);
        }
    }

    public void updateTable(Table table) {
        table.createOrUpdate(this.connection());
    }

    public void addTable(Table table) {
        this.tables.add(table);
        table.createOrUpdate(this.connection());
    }

    public void dropTable(Table table) {
        try (Statement statement = this.connection().createStatement()) {
            table.dropTable(statement);
        } catch (SQLException e) {
            Main.getInstance().getLogger().log(Level.WARNING, "Failed to drop table: " + table.name + " (might not exist)");
            throw new RuntimeException(e);
        }
    }

    public Connection connection() {
        return connection;
    }


    public Set<Table> getTables() {
        return tables;
    }
}
