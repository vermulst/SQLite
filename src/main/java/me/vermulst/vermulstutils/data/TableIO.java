package me.vermulst.vermulstutils.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableIO<PK> implements AutoCloseable {

    protected final Table<PK> table;
    protected Connection connection;
    protected TableIO(Table table) {
        this.table = table;
    }

    protected String getPrimaryKeyCondition(PK primaryKey) {
        List<String> primaryKeyNames = this.table.getPrimaryKeyNames().stream().toList();
        StringBuilder condition = new StringBuilder(" WHERE ");
        List<Object> objects = new ArrayList<>();
        if (primaryKey instanceof CompositeKey compositeKey) {
            objects.addAll(compositeKey.getKeyParts());
        } else {
            objects.add(primaryKey);
        }
        for (int i = 0; i < primaryKeyNames.size(); i++) {
            if (i != 0) {
                condition.append(" AND ");
            }
            condition.append(primaryKeyNames.get(i)).append(" = '").append(objects.get(i)).append("'");
        }
        return condition.toString();
    }

    protected boolean entryExists(PK primaryKey) {
        if (this.isDisconnected()) return false;
        List<String> primaryKeyNames = this.table.getPrimaryKeyNames().stream().toList();
        if (primaryKeyNames.size() != this.table.getPrimaryKeySize(primaryKey)) {
            throw new IllegalArgumentException("Primary key values do not match the number of primary key columns.");
        }
        StringBuilder query = new StringBuilder("SELECT EXISTS(SELECT 1 FROM ");
        query.append(this.table.name).append(this.getPrimaryKeyCondition(primaryKey));
        query.append(" LIMIT 1)");
        try (Statement statement = connection.createStatement()){
            ResultSet resultSet = statement.executeQuery(query.toString());
            return resultSet.getBoolean(1);
        } catch (SQLException e) {
            throw new RuntimeException("Error checking if entry exists", e);
        }
    }

    protected void connect(Database database) {
        this.connection = database.getConnection();
    }

    public void disconnect(Database database) {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        database.closeConnection();
    }

    protected boolean isDisconnected() {
        return this.connection == null;
    }

    @Override
    public void close() throws Exception {
        if (this.connection == null || this.connection.isClosed()) return;
        this.connection.close();
    }
}
