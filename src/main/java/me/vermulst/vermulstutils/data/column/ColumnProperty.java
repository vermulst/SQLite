package me.vermulst.vermulstutils.data.column;

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
            return "NOT_NULL";
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
    }
}
