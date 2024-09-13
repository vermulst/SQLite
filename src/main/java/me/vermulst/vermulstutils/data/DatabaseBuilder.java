package me.vermulst.vermulstutils.data;

import java.util.HashSet;
import java.util.Set;

public class DatabaseBuilder {

    private String path;
    private Set<TableBuilder> builders = new HashSet<>();

    public DatabaseBuilder path(String path) {
        this.path = path;
        return this;
    }

    public DatabaseBuilder tables(TableBuilder... tableBuilders) {
        this.builders = Set.of(tableBuilders);
        return this;
    }

    public Database build() {
        Database database = new Database();
        database.initiateConnection(this.path);
        database.tables(this.builders);
        return database;
    }

}
