package me.vermulst.vermulstutils.data;

public class Column {


    private String name;
    private final ColumnType columnType;
    private final boolean notNull;
    public Column(String name, ColumnType columnType, boolean notNull) {
        this.name = name;
        this.columnType = columnType;
        this.notNull = notNull;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public String getName() {
        return name;
    }

    public ColumnType getColumnType() {
        return columnType;
    }
}
