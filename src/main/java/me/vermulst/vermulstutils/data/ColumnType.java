package me.vermulst.vermulstutils.data;

public enum ColumnType implements ColumnTypeName {
    SHORT {
        @Override
        public String getColumnTypeName() {
            return "SMALLINT";
        }
    },
    LONG {
        @Override
        public String getColumnTypeName() {
            return "BIGINT";
        }
    },
    INTEGER {
        @Override
        public String getColumnTypeName() {
            return "INTEGER";
        }
    },
    FLOAT {
        @Override
        public String getColumnTypeName() {
            return "REAL";
        }
    },
    DOUBLE {
        @Override
        public String getColumnTypeName() {
            return "REAL";
        }
    },
    STRING {
        @Override
        public String getColumnTypeName() {
            return "TEXT";
        }
    },
    BOOLEAN {
        @Override
        public String getColumnTypeName() {
            return "BOOLEAN";
        }
    }
}
