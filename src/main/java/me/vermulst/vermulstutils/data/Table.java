package me.vermulst.vermulstutils.data;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static me.vermulst.vermulstutils.data.Column.REVERSED_TYPE_MAP_STRING;

public class Table<PK> {

    protected String name;
    protected List<Column<?>> columns = new ArrayList<>();
    protected Class<PK> type;

    // Foreign key map: foreign key column -> referenced table
    protected final Set<ForeignKeyReference> foreignKeyReferences = new HashSet<>();

    protected Table(Class<PK> type) {
        this.type = type;
    }

    public <T> Column<T> getColumn(Class<T> columnType, String name) {
        Optional<Column<?>> columnOptional = columns.stream()
                .filter(column1 -> name.equalsIgnoreCase(column1.getName()) && column1.getType().equals(columnType))
                .findFirst();
        return (Column<T>) columnOptional.orElse(null);
    }

    protected void createOrUpdate(Connection connection) {
        boolean update = false;
        try (Statement statement = connection.createStatement()) {
            boolean tableExists = this.tableExists(statement);
            if (!tableExists) {
                this.create(statement);
            } else {
                boolean tableChanged = this.tableChanged(statement);
                if (!tableChanged) return;
                update = true;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed creating statement for table: " + this.name, e);
        }
        if (update) {
            System.out.println("updating");
            this.update(connection);
        }
    }

    private void create(Statement statement) {
        try {
            statement.execute(this.getCreateStatement());
        } catch (SQLException e) {
            throw new RuntimeException("Failed initiating table: " + this.name, e);
        }
    }

    private void update(Connection connection) {
        this.name += "_new";
        String newTable = this.getCreateStatement();
        this.name = this.name.substring(0, this.name.length() - 4);
        try {
            this.createNewTable(newTable, connection);
            this.cutOldTable(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tableExists(Statement statement) {
        String checkTableExistsQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + this.name + "';";
        try (ResultSet resultSet = statement.executeQuery(checkTableExistsQuery)) {
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException("Error checking if table exists: " + this.name, e);
        }
    }

    /**
     * Checks if the table structure has changed in the database compared to the defined structure.
     */
    public boolean tableChanged(Statement statement) {
        Set<String> currentColumnNames = new HashSet<>();
        Set<String> currentColumnTypes = new HashSet<>();
        Map<String, Set<Column.ColumnProperty>> currentColumnProperties = new HashMap<>();
        Map<String, Object> defaultValues = new HashMap<>();

        try {
            ResultSet tableSql = statement.executeQuery("SELECT sql FROM sqlite_master WHERE type='table' AND name='" + this.name + "'");
            if (!tableSql.next()) {
                throw new RuntimeException("Table " + this.name + " does not exist in the database.");
            }
            String createTableSql = tableSql.getString("sql");
            // Extract column definitions from the CREATE TABLE statement
            String columnRegex = "(\\w+)\\s+([\\w\\s]+)(?:DEFAULT\\s+([\\w\\d]+))?";
            Pattern columnPattern = Pattern.compile(columnRegex);
            Matcher matcher = columnPattern.matcher(createTableSql);
            Pattern pkPattern = Pattern.compile("PRIMARY KEY\\s*\\(([^\\)]+)\\)", Pattern.CASE_INSENSITIVE);
            Matcher primaryKeyMatcher = pkPattern.matcher(createTableSql);

            if (primaryKeyMatcher.find()) {
                String[] primaryKeyColumns = primaryKeyMatcher.group(1).split(",");
                for (String primaryKey : primaryKeyColumns) {
                    primaryKey = primaryKey.trim();
                    currentColumnProperties.put(primaryKey, new HashSet<>(List.of(Column.ColumnProperty.PRIMARY_KEY)));
                }
            }

            while (matcher.find()) {
                String columnName = matcher.group(1);
                String[] typeAndConstraints = matcher.group(2).trim().split(" ");
                if (columnName.equals("PRIMARY") || columnName.equals("CREATE")) continue;
                currentColumnNames.add(columnName);
                String columnType = typeAndConstraints[0];
                currentColumnTypes.add(columnType);
                int endIndex = typeAndConstraints.length;
                if (typeAndConstraints.length >= 2 && typeAndConstraints[typeAndConstraints.length - 2].equals("DEFAULT")) {
                    endIndex -= 2;
                    String defaultValueString = typeAndConstraints[typeAndConstraints.length - 1];
                    Class typeCast = REVERSED_TYPE_MAP_STRING.get(columnType);
                    Method castMethod = typeCast.getDeclaredMethod("valueOf", String.class);
                    Object defaultValue = castMethod.invoke(typeCast, defaultValueString);
                    defaultValues.put(columnName, defaultValue);
                } else {
                    defaultValues.put(columnName, "NO_DEFAULT");
                }
                Set<Column.ColumnProperty> columnProperties = new HashSet<>();
                for (int i = 1; i < endIndex; i++) {
                    String type = typeAndConstraints[i];
                    if ("NOT".equals(type)) {
                        columnProperties.add(Column.ColumnProperty.NOT_NULL);
                        i++;
                    } else if ("UNIQUE".equals(type)) {
                        columnProperties.add(Column.ColumnProperty.UNIQUE);
                    }
                }
                if (!currentColumnProperties.containsKey(columnName)) {
                    currentColumnProperties.put(columnName, new HashSet<>());
                }
                currentColumnProperties.get(columnName).addAll(columnProperties);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table info: " + this.name, e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Compare column names
        Set<String> definedColumnNames = this.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        // Compare column types
        Set<String> definedColumnTypes = this.columns.stream()
                .map(Column::getColumnTypeName)
                .collect(Collectors.toSet());

        Map<String, Set<Column.ColumnProperty>> definedColumnProperties = this.columns.stream()
                .collect(Collectors.toMap(
                        Column::getName,                  // Key: column name
                        Column::getColumnProperties      // Value: column properties
                ));


        Map<String, Object> definedDefaultValues = this.columns.stream()
                .collect(Collectors.toMap(
                        Column::getName,                  // Key: column name
                        column -> column.getDefaultValue() != null
                                ? column.getDefaultValue()
                                : "NO_DEFAULT"          // Replace null with a placeholder
                ));

        // Compare column names and types to check if any changes occurred
        boolean namesChanged = !currentColumnNames.equals(definedColumnNames);
        boolean typesChanged = !currentColumnTypes.equals(definedColumnTypes);
        boolean propertiesChanged = !currentColumnProperties.equals(definedColumnProperties);
        boolean defaultValueChanged = !defaultValues.equals(definedDefaultValues);

        return namesChanged || typesChanged || propertiesChanged || defaultValueChanged;
    }

    public String getCreateStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ").append(this.name).append(" (");
        int totalSize = this.columns.size();
        int count = 0;

        for (Column<?> column : this.columns) {
            count++;
            builder.append(column.getColumnDefinition());
            if (count != totalSize) builder.append(", ");
        }

        List<String> primaryKeys = this.getPrimaryKeyNames();
        if (!primaryKeys.isEmpty()) {
            builder.append(", PRIMARY KEY (");
            builder.append(String.join(", ", primaryKeys));
            builder.append(")");
        }

        builder.append(this.getForeignKeyConstraints());

        builder.append(");");
        return builder.toString();
    }


    protected List<Object> getPrimaryKeyObjects(PK primaryKey) {
        return primaryKey instanceof CompositeKey compositeKey ? compositeKey.getKeyParts() : List.of(primaryKey);
    }

    protected int getPrimaryKeySize(PK primaryKey) {
        if (primaryKey instanceof CompositeKey compositeKey) {
            return compositeKey.getKeyParts().size();
        }
        return primaryKey != null ? 1 : 0;
    }

    protected List<String> getPrimaryKeyNames() {
        return this.columns
                .stream().filter(Column::isPrimaryKey)
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    protected String getForeignKeyConstraints() {
        StringBuilder foreignKeysSql = new StringBuilder();
        for (ForeignKeyReference foreignKey : foreignKeyReferences) {
            foreignKeysSql.append(foreignKey.getStatement()).append(", ");
        }
        if (!foreignKeysSql.isEmpty()) {
            // Remove trailing comma
            foreignKeysSql.setLength(foreignKeysSql.length() - 2);
        }
        return foreignKeysSql.toString();
    }

    protected List<Column<?>> getPrimaryKeyColumns() {
        return this.columns.stream().filter(Column::isPrimaryKey).collect(Collectors.toList());
    }

    private void createNewTable(String newTable, Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(newTable);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private void cutOldTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            this.copyOldTableIntoNew(statement);
            this.dropTable(statement);
            this.renameNewTable(statement);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private void copyOldTableIntoNew(Statement statement) throws SQLException {
        List<String> columnNames = this.getCopyingColumns(statement);
        String columns = String.join(", ", columnNames);
        String copyStatement = "INSERT INTO " + this.name + "_new (" + columns +
                ") SELECT " + columns + " FROM " + this.name + ";";
        statement.execute(copyStatement);
    }

    protected void dropTable(Statement statement) throws SQLException {
        String drop = "DROP TABLE " + this.name + ";";
        statement.execute(drop);
    }

    private void renameNewTable(Statement statement) throws SQLException {
        String renameTableSQL = "ALTER TABLE " + this.name + "_new RENAME TO " + this.name + ";";
        statement.execute(renameTableSQL);
    }

    private List<String> getCopyingColumns(Statement statement) throws SQLException {
        List<String> columnNames = this.columns.stream().map(Column::getName).collect(Collectors.toList());
        ResultSet oldTableColumns = statement.executeQuery("PRAGMA table_info(" + this.name + ")");
        Set<String> oldTableColumnNames = new HashSet<>();
        while (oldTableColumns.next()) {
            oldTableColumnNames.add(oldTableColumns.getString("name"));
        }
        columnNames.removeIf(columnName -> !oldTableColumnNames.contains(columnName));
        return columnNames;
    }

    public String getName() {
        return name;
    }

    public List<Column<?>> getColumns() {
        return columns;
    }

    public Class<PK> getType() {
        return type;
    }

    public Builder<PK> toBuilder() {
        return new Builder<>(this.type)
                .name(this.name)
                .columns(columns);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Table<?> table = (Table<?>) object;
        return Objects.equals(name, table.name) && Objects.equals(columns, table.columns) && Objects.equals(type, table.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns, type);
    }

    public static <PK> Builder<PK> builder(Class<PK> primaryKeyType) {
        return new Builder<>(primaryKeyType);
    }

    public static <PK> Loader<PK> loader(Table table, Database database) {
        Loader loader = new Loader<>(table);
        loader.connect(database);
        return loader;
    }

    public static <PK> Saver<PK> saver(Table table, Database database) {
        Saver saver = new Saver<>(table);
        saver.connect(database);
        return saver;
    }

    public static <PK> Deleter<PK> deleter(Table table, Database database) {
        Deleter deleter = new Deleter(table);
        deleter.connect(database);
        return deleter;
    }

    public static class Builder<PK> {

        private String name;
        private List<Column<?>> columns = new ArrayList<>();
        private final Class<PK> primaryKeyType;

        private Set<ForeignKeyReference> foreignKeyReferences = new HashSet<>();

        private Builder(Class<PK> primaryKeyType) {
            this.primaryKeyType = primaryKeyType;
        }

        protected Builder<PK> findColumns(Connection connection) {
            try {
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet rs = metaData.getColumns(null, null, name, "%");

                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int dataType = rs.getInt("DATA_TYPE");
                    if ("BIGINT".equalsIgnoreCase(rs.getString("TYPE_NAME")) && dataType == 4) {
                        dataType = Types.BIGINT; // Correct the type
                    }
                    boolean isNotNull = rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                    boolean isPrimaryKey = isPrimaryKey(connection, columnName);
                    boolean isUnique = isUnique(connection, columnName);

                    Column.Builder columnBuilder = Column.builder(Column.REVERSED_TYPE_MAP.get(dataType))
                            .name(columnName);

                    if (isNotNull) columnBuilder.addColumnProperty(Column.ColumnProperty.NOT_NULL);
                    if (isPrimaryKey) columnBuilder.addColumnProperty(Column.ColumnProperty.PRIMARY_KEY);
                    if (isUnique) columnBuilder.addColumnProperty(Column.ColumnProperty.UNIQUE);

                    // Add the column to the table

                    this.columns.add(columnBuilder.build());
                }

                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to retrieve columns for table: " + name, e);
            }
            return this;
        }

        protected Builder<PK> findForeignKeys(Connection connection) {
            try {
                DatabaseMetaData metaData = connection.getMetaData();

                // Query foreign keys for the current table
                ResultSet foreignKeyRs = metaData.getImportedKeys(null, null, name);

                while (foreignKeyRs.next()) {
                    String fkColumnName = foreignKeyRs.getString("FKCOLUMN_NAME"); // Child column (foreign key)
                    String pkTableName = foreignKeyRs.getString("PKTABLE_NAME"); // Referenced table (parent)
                    String pkColumnName = foreignKeyRs.getString("PKCOLUMN_NAME"); // Parent column (primary key)

                    // Add to the foreign key references set
                    foreignKeyReferences.add(new ForeignKeyReference(fkColumnName, pkTableName, pkColumnName));
                }

                foreignKeyRs.close();

            } catch (SQLException e) {
                throw new RuntimeException("Failed to retrieve foreign keys for table: " + name, e);
            }
            return this;
        }

        private boolean isUnique(Connection connection, String columnName) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet indexRs = metaData.getIndexInfo(null, null, name, true, false);

            while (indexRs.next()) {
                String indexedColumn = indexRs.getString("COLUMN_NAME");
                boolean isUnique = !indexRs.getBoolean("NON_UNIQUE");

                if (columnName.equals(indexedColumn) && isUnique) {
                    indexRs.close();
                    return true;
                }
            }

            indexRs.close();
            return false;
        }

        private boolean isPrimaryKey(Connection connection, String columnName) throws SQLException {
            ResultSet pkResultSet = connection.getMetaData().getPrimaryKeys(null, null, name);
            while (pkResultSet.next()) {
                String pkColumnName = pkResultSet.getString("COLUMN_NAME");
                if (pkColumnName.equals(columnName)) {
                    pkResultSet.close();
                    return true;
                }
            }
            pkResultSet.close();
            return false;
        }

        public Builder<PK> foreignKeys(ForeignKeyReference... foreignKeyReferences) {
            this.foreignKeyReferences = Set.of(foreignKeyReferences);
            return this;
        }

        public Builder<PK> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<PK> columns(Column<?>... columns) {
            this.columns = List.of(columns);
            return this;
        }

        public Builder<PK> columns(List<Column<?>> columns) {
            this.columns = columns;
            return this;
        }

        public Builder<PK> columnBuilders(Column.Builder... columnBuilders) {
            return this.columnBuilders(List.of(columnBuilders));
        }

        public Builder<PK> columnBuilders(List<Column.Builder> columnBuilders) {
            List<Column<?>> columns = new ArrayList<>();
            for (Column.Builder columnBuilder : columnBuilders) {
                Column column = columnBuilder.build();
                if (column.getColumnProperties().contains(Column.ColumnProperty.UNIQUE)) {

                }
                columns.add(columnBuilder.build());
            }
            this.columns = columns;
            return this;
        }

        public Builder<PK> addColumn(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder<PK> addColumn(Column.Builder columnBuilder) {
            this.columns.add(columnBuilder.build());
            return this;
        }

        public List<Column<?>> getColumns() {
            return columns;
        }

        protected Table build(Connection connection) {
            Table table = new Table(this.primaryKeyType);
            table.name = this.name;
            table.columns = this.columns;
            table.createOrUpdate(connection);
            return table;
        }

        public String getName() {
            return name;
        }
    }


    public static class Loader<PK> extends TableIO<PK> implements AutoCloseable {

        private Loader(Table table) {
            super(table);
        }

        public List<Object> loadRow(PK primaryKey) {
            if (this.isDisconnected()) return null;
            List<Object> values = new ArrayList<>();
            for (Column column : this.table.getColumns()) {
                if (column.isPrimaryKey()) continue;
                values.add(this.loadValue(column, primaryKey));
            }
            return values;
        }

        public <T> T loadValue(Column<T> column, PK primaryKey) {
            if (this.isDisconnected()) return null;
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(column.getName()).append(" FROM ").append(this.table.name);
            sql.append(this.getPrimaryKeyCondition(primaryKey));
            try (Statement statement = connection.createStatement()) {
                ResultSet result = statement.executeQuery(sql.toString());
                if (result.next()) {
                    try {
                        return result.getObject(1, column.getType()); // Use conversion
                    } catch (SQLException e) {
                        if (result.wasNull()) { // Check explicitly for NULL handling
                            return null;
                        }
                        throw e; // Rethrow other SQLExceptions
                    }
                } else {
                    return null;
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed loading value", e);
            }
        }

        public <T> Map<PK, T> loadColumn(Column<T> column) {
            Map<PK, T> resultMap = new HashMap<>();
            if (this.isDisconnected()) return resultMap;
            Class<T> columnType = column.getType();
            List<String> primaryKeys = this.table.getPrimaryKeyNames();
            String primaryKeyNames = String.join(", ", primaryKeys);
            String columnName = column.getName();
            String sql = "SELECT " + primaryKeyNames + ", " + columnName + " FROM " + this.table.name;
            try (PreparedStatement pstmt = connection.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    List<Object> results = primaryKeys.stream().map(primaryKey -> {
                        try {
                            return rs.getObject(primaryKey);
                        } catch (SQLException e) {
                            throw new RuntimeException("Error fetching primary key", e);
                        }
                    }).toList();
                    T columnValue = rs.getObject(columnName, columnType);
                    PK primaryKey = results.size() > 1 ? (PK) results : results.size() == 1 ? (PK) results.get(0) : null;
                    if (primaryKey == null) continue;
                    resultMap.put(primaryKey, columnValue);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Error executing query", e);
            }
            return resultMap;
        }
    }


    public static class Saver<PK> extends TableIO<PK> {

        private Saver(Table table) {
            super(table);
        }

        public void saveEntry(PK primaryKey) {
            if (this.isDisconnected()) return;
            boolean exists = this.entryExists(primaryKey);
            if (exists) return;
            List<String> primaryKeyNames = this.table.getPrimaryKeyNames();
            if (primaryKeyNames.size() != this.table.getPrimaryKeySize(primaryKey)) {
                throw new IllegalArgumentException("Primary key values do not match the number of primary key columns.");
            }
            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(this.table.name).append(" (");
            sql.append(String.join(", ", primaryKeyNames));
            sql.append(") VALUES (");
            sql.append("?, ".repeat(Math.max(0, primaryKeyNames.size() - 1)));
            sql.append("?)");
            try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
                int index = 1;
                List<Object> keys = this.table.getPrimaryKeyObjects(primaryKey);
                for (Object primaryKeyValue : keys) {
                    pstmt.setObject(index++, primaryKeyValue);
                }
                pstmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("Error inserting new entry into the table", e);
            }
        }

        /** Insert values into a column vertically
         *
         * @param column - the column to insert
         * @param values - Key = primary key, value = value in the column
         * @param <T> - type of column
         */
        public <T> void saveColumn(Column<T> column, Map<PK, T> values) {
            if (this.isDisconnected()) return;
            if (values.keySet().size() != this.table.getPrimaryKeyNames().size()) {
                throw new IllegalArgumentException("Primary key values do not match the number of primary key columns.");
            }
            for (Map.Entry<PK, T> entry : values.entrySet()) {
                this.saveValue(column, entry.getKey(), entry.getValue());
            }
        }

        /** Insert values into a row horizontally
         *
         * @param primaryKey - primary key of the row
         * @param values - values in same order to the columns
         */
        public void saveRow(PK primaryKey, List<Object> values) {
            List<Column<?>> columns = this.table.getColumns();
            this.saveRow(primaryKey, columns, values);
        }

        /** Insert values into a row horizontally
         *
         * @param columns - columns to insert into
         * @param values - values in same order to the columns
         */
        public void saveRow(PK primaryKey, List<Column<?>> columns, List<Object> values) {
            if (this.isDisconnected()) return;
            this.saveEntry(primaryKey);
            columns = new ArrayList<>(columns);
            columns.removeAll(this.table.getPrimaryKeyColumns());
            if (columns.size() != values.size()) {
                throw new IllegalArgumentException("Inserted values do not match the number of columns.");
            }
            for (int i = 0; i < columns.size(); i++) {
                Column<?> column = columns.get(i);
                Object value = values.get(i);
                if (!column.getType().isInstance(value)) {
                    throw new IllegalArgumentException("Value type does not match the column type.");
                }
                this.saveValue((Column<Object>) column, primaryKey, value);
            }
        }

        public <T> void saveValue(Column<T> column, PK primaryKey, T value) {
            if (this.isDisconnected()) return;
            this.saveEntry(primaryKey);
            List<String> primaryKeyNames = this.table.getPrimaryKeyNames();
            if (primaryKeyNames.size() != this.table.getPrimaryKeySize(primaryKey)) {
                throw new IllegalArgumentException("Primary key values do not match the number of primary key columns.");
            }
            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append(this.table.name).append(" SET ");
            sql.append(column.getName()).append(" = ").append(value);
            sql.append(this.getPrimaryKeyCondition(primaryKey));
            try (Statement statement = this.connection.createStatement()) {
                statement.executeUpdate(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException("Error inserting value into the table", e);
            }
        }
    }


    public static class Deleter<PK> extends TableIO<PK> {
        private Deleter(Table table) {
            super(table);
        }

        public void deleteEntry(PK primaryKey) {
            String sql = "DELETE FROM " + this.table.name + this.getPrimaryKeyCondition(primaryKey);
            try (Statement statement = this.connection.createStatement()) {
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException("Error inserting value into the table", e);
            }
        }
    }


}
