package me.vermulst.vermulstutils.data;

public class ForeignKeyReference {

    private final String childColumnName;
    private final String parentTableName;
    private final String parentColumnName;

    public ForeignKeyReference(String childColumnName, String parentTableName, String parentColumnName) {
        this.childColumnName = childColumnName;
        this.parentTableName = parentTableName;
        this.parentColumnName = parentColumnName;
    }

    public String getStatement() {
        return "FOREIGN KEY (" +
                this.childColumnName +
                ") REFERENCES " +
                this.parentTableName +
                " (" +
                this.parentColumnName +
                ") ON DELETE CASCADE";
    }

    public String getChildColumnName() {
        return childColumnName;
    }

    public String getParentTableName() {
        return parentTableName;
    }

    public String getParentColumnName() {
        return parentColumnName;
    }
}
