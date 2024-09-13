package me.vermulst.vermulstutils.data;

import me.vermulst.vermulstutils.data.column.Column;
import me.vermulst.vermulstutils.data.column.ColumnBuilder;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

public class TableBuilder {


    private String name;
    private Set<Column> columns = new HashSet<>();

    public TableBuilder name(String name) {
        this.name = name;
        return this;
    }

    public TableBuilder columns(Column... columns) {
        this.columns = Set.of(columns);
        return this;
    }

    public TableBuilder columns(Set<Column> columns) {
        this.columns = columns;
        return this;
    }

    public TableBuilder columnBuilders(ColumnBuilder... columnBuilders) {
        return this.columnBuilders(Set.of(columnBuilders));
    }

    public TableBuilder columnBuilders(Set<ColumnBuilder> columnBuilders) {
        Set<Column> columns = new HashSet<>();
        for (ColumnBuilder columnBuilder : columnBuilders) {
            columns.add(columnBuilder.build());
        }
        this.columns = columns;
        return this;
    }

    public TableBuilder addColumn(Column column) {
        this.columns.add(column);
        return this;
    }

    public TableBuilder addColumn(ColumnBuilder columnBuilder) {
        this.columns.add(columnBuilder.build());
        return this;
    }

    protected Table build(Connection connection) {
        Table table = new Table();
        table.name = this.name;
        table.columns = this.columns;
        table.createOrUpdate(connection);
        return table;
    }


}
