package me.vermulst.vermulstutils.data;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Column<T> {

    protected static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();
    protected static final Map<Integer, Class<?>> REVERSED_TYPE_MAP;

    static {
        TYPE_MAP.put(Integer.class, "INTEGER");
        TYPE_MAP.put(Short.class, "SMALLINT");
        TYPE_MAP.put(Long.class, "BIGINT");
        TYPE_MAP.put(Float.class, "REAL");
        TYPE_MAP.put(Double.class, "DOUBLE");
        TYPE_MAP.put(Boolean.class, "BOOLEAN");
        TYPE_MAP.put(String.class, "TEXT");

        REVERSED_TYPE_MAP = new HashMap<>();
        REVERSED_TYPE_MAP.put(Types.INTEGER, Integer.class);
        REVERSED_TYPE_MAP.put(Types.BIGINT, Long.class);
        REVERSED_TYPE_MAP.put(Types.VARCHAR, String.class);
        REVERSED_TYPE_MAP.put(Types.CHAR, String.class);
        REVERSED_TYPE_MAP.put(Types.BOOLEAN, Boolean.class);
        REVERSED_TYPE_MAP.put(Types.FLOAT, Float.class);
        REVERSED_TYPE_MAP.put(Types.DOUBLE, Double.class);
    }

    private interface ColumnPropertyDefinition {
        String getDefinition();
    }

    public enum ColumnProperty implements ColumnPropertyDefinition {
        UNIQUE {
            @Override
            public String getDefinition() {
                return "UNIQUE";
            }
        },
        NOT_NULL {
            @Override
            public String getDefinition() {
                return "NOT NULL";
            }
        },
        PRIMARY_KEY {
            @Override
            public String getDefinition() {
                return "";
            }
        },
        AUTO_INCREMENT_PRIMARY_KEY {
            @Override
            public String getDefinition() {
                return "AUTOINCREMENT";
            }
        };
    }

    protected String name;
    protected Set<ColumnProperty> columnProperties = new HashSet<>();
    protected T defaultValue;
    protected Class<T> type;

    protected Column() {
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

    protected String getColumnDefinition() {
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

    protected String getColumnTypeName() {
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

    public Class<T> getType() {
        return type;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public Builder toBuilder() {
        return new Builder(type)
                .name(name)
                .columnProperties(columnProperties)
                .defaultValue(defaultValue);
    }

    public static <T> Builder<T> builder(Class<T> type) {
        return new Builder(type);
    }

    public static class Builder<T> {

        private String name;
        private Set<ColumnProperty> columnProperties;
        private T defaultValue;
        private final Class<T> type;

        private Builder(Class<T> type) {
            this.type = type;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> defaultValue(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder<T> columnProperties(ColumnProperty... columnProperties) {
            this.columnProperties = Set.of(columnProperties);
            return this;
        }

        public Builder<T> columnProperties(Set<ColumnProperty> columnProperties) {
            this.columnProperties = columnProperties;
            return this;
        }

        public Builder<T> addColumnProperty(ColumnProperty columnProperty) {
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
            column.columnProperties = this.columnProperties != null ? this.columnProperties : new HashSet<>();
            column.defaultValue = this.defaultValue;
            column.type = this.type;
            return column;
        }

    }


}
