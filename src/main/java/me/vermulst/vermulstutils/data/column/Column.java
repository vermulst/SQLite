package me.vermulst.vermulstutils.data.column;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Column<T> {

    private static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();

    static {
        TYPE_MAP.put(Integer.class, "INTEGER");
        TYPE_MAP.put(Short.class, "SMALLINT");
        TYPE_MAP.put(Long.class, "BIGINT");
        TYPE_MAP.put(Float.class, "REAL");
        TYPE_MAP.put(Double.class, "REAL");
        TYPE_MAP.put(String.class, "TEXT");
        TYPE_MAP.put(Boolean.class, "BOOLEAN");
    }

    protected String name;
    protected Set<ColumnProperty> columnProperties;
    protected T defaultValue;
    protected Class<T> type;

    protected Column() {

    }

    public ColumnBuilder builder() {
        return new ColumnBuilder(type)
                .name(name)
                .columnProperties(columnProperties)
                .defaultValue(defaultValue);
    }

    public String getName() {
        return name;
    }

    public Set<ColumnProperty> getColumnProperties() {
        return columnProperties;
    }

    public boolean isPrimaryKey() {
        return this.columnProperties.contains(ColumnProperty.PRIMARY_KEY) || this.columnProperties.contains(ColumnProperty.AUTO_INCREMENT_PRIMARY_KEY);
    }

    public String getColumnDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(" ").append(this.getColumnTypeName());
        for (ColumnProperty columnProperty : this.columnProperties) {
            String definition = columnProperty.getDefinition();
            if (definition.isEmpty()) continue;
            sb.append(" ").append(columnProperty.getDefinition());
        }
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(defaultValue));
        }
        return sb.toString();
    }

    public String getColumnTypeName() {
        String typeName = TYPE_MAP.get(type);
        return typeName != null ? typeName : "TEXT"; // Default to TEXT if type not found
    }

    private String formatDefaultValue(T defaultValue) {
        if (defaultValue instanceof String) {
            return "'" + defaultValue + "'";
        } else if (defaultValue instanceof Boolean) {
            return (Boolean) defaultValue ? "1" : "0";
        }
        return defaultValue.toString();
    }


}
