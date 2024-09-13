package me.vermulst.vermulstutils.data.column;

import java.util.HashSet;
import java.util.Set;

public class ColumnBuilder<T> {

    private String name;
    private Set<ColumnProperty> columnProperties;
    private T defaultValue;
    private final Class<T> type;

    public ColumnBuilder(Class<T> type) {
        this.type = type;
    }

    public ColumnBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    public ColumnBuilder<T> defaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public ColumnBuilder<T> columnProperties(ColumnProperty... columnProperties) {
        this.columnProperties = Set.of(columnProperties);
        return this;
    }

    public ColumnBuilder<T> columnProperties(Set<ColumnProperty> columnProperties) {
        this.columnProperties = columnProperties;
        return this;
    }

    public ColumnBuilder<T> addColumnProperty(ColumnProperty columnProperty) {
        if (this.columnProperties == null) this.columnProperties = new HashSet<>();
        this.columnProperties.add(columnProperty);
        return this;
    }

    public Column<T> build() {
        if (this.name == null || this.type == null) {
            throw new IllegalStateException("Name and type must be set.");
        }
        Column column = new Column<>();
        column.name = this.name;
        column.columnProperties = this.columnProperties;
        column.defaultValue = this.defaultValue;
        column.type = this.type;
        return column;
    }

}
