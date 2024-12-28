package test;

import me.vermulst.vermulstutils.data.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IOTest {

    @Test
    public void testSave1() {

        Column.Builder uuidColumn = Column.builder(String.class)
                .name("UUID")
                .columnProperties(
                        Column.ColumnProperty.UNIQUE,
                        Column.ColumnProperty.PRIMARY_KEY,
                        Column.ColumnProperty.NOT_NULL);
        Column.Builder goldColumn = Column.builder(Long.class)
                .name("GOLD")
                .columnProperties(Column.ColumnProperty.NOT_NULL)
                .defaultValue(10L);
        Column.Builder xpColumn = Column.builder(Long.class)
                .name("XP")
                .columnProperties(Column.ColumnProperty.NOT_NULL)
                .defaultValue(500L);

        Table.Builder playerData = Table.builder(String.class)
                .name("player_data")
                .columnBuilders(uuidColumn, goldColumn, xpColumn);


        Database db = Database.builder()
                .path("test1")
                .addAndOverride(playerData)
                .build();

        if (db.getTables().size() == db.getTables().size()) {
            Table[] db1Array = db.getTables().toArray(new Table[0]);
            Table[] db2Array = db.getTables().toArray(new Table[0]);
            for (int i = 0; i < db.getTables().size(); i++) {
                Table table1 = db1Array[i];
                System.out.println(table1.getCreateStatement());
                Table table2 = db2Array[i];
                if (table1.getColumns().size() == table2.getColumns().size()) {
                    Object[] table1ColumnArray = table1.getColumns().toArray(new Column<?>[0]);
                    Object[] table2ColumnArray = table2.getColumns().toArray(new Column<?>[0]);
                    for (int j = 0; j < table1.getColumns().size(); j++) {
                        Object obj1 = table1ColumnArray[j];
                        Object obj2 = table2ColumnArray[j];
                        if (!(obj1 instanceof Column<?> column1) || !(obj2 instanceof Column<?> column2)) {
                            System.out.println("NOT OF TYPE COLUMN?????");
                        } else {
                            System.out.println("COLUMN 1: " + column1.getName());
                            System.out.println("type: " + column1.getType());
                            System.out.println("prop: " + Arrays.toString(column1.getColumnProperties().toArray()));
                            System.out.println("Default value: " + column1.getDefaultValue());
                            System.out.println("\nVS\n");
                            System.out.println("COLUMN 2: " + column2.getName());
                            System.out.println("type: " + column2.getType());
                            System.out.println("prop: " + Arrays.toString(column2.getColumnProperties().toArray()));
                            System.out.println("Default value: " + column2.getDefaultValue());
                            System.out.println("EQUAL?: " + column1.equals(column2));
                        }
                    }
                } else {
                    System.out.println("TABLE SIZE DOES NOT MATCH SIZE: ");
                    System.out.println(table1.getName());
                    System.out.println(table2.getName());
                }
            }
        } else {
            System.out.println("DB DOES NOT MATCH SIZE");
        }
        assertTrue(db.getTables().equals(db.getTables()));
        assertTrue(db.equals(db));

        Table table = db.getTable(String.class, playerData.getName());

        Long randomValue = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        try (Table.Saver<String> saver = Table.saver(table, db)) {
            saver.saveEntry("TEST4");
            saver.saveRow("TEST5", List.of(randomValue, 1L));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (Object obj : table.getColumns()) {
            if (obj instanceof Column<?> column) {
                System.out.println("\n");
                System.out.println(column.getName());
                System.out.println(Arrays.toString(column.getColumnProperties().toArray()));
                column.getColumnProperties().remove(Column.ColumnProperty.NOT_NULL);
                System.out.println(Arrays.toString(column.getColumnProperties().toArray()));
            }
        }
        try {
            System.out.println(table.tableChanged(db.getConnection().createStatement()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        Table.Loader<String> loader = Table.loader(table, db);
        List<Object> objects = loader.loadRow("TEST5");
        assertEquals(List.of(randomValue).getFirst(), objects.getFirst());
        List<Object> objects1 = loader.loadRow("TEST2");
        //assertEquals(null, objects1.getLast());
        loader.disconnect(db);

    }


    @Test
    public void testSave2() {

        Column.Builder uuidColumn = Column.builder(String.class)
                .name("UUID")
                .columnProperties(Column.ColumnProperty.UNIQUE, Column.ColumnProperty.PRIMARY_KEY, Column.ColumnProperty.NOT_NULL);
        Column.Builder goldColumn = Column.builder(Long.class)
                .name("GOLD")
                .columnProperties(Column.ColumnProperty.NOT_NULL)
                .defaultValue(10L);

        Table.Builder playerData = Table.builder(String.class)
                .name("player_data")
                .columnBuilders(uuidColumn, goldColumn);

        Database db = Database.builder()
                .path("test2")
                .addTablesIfNotExists(playerData)
                .build();

        Table table = db.getTable(String.class, playerData.getName());
        Table.Saver<String> saver = Table.saver(table, db);
        saver.saveEntry("TEST1");
        Long randomValue = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        saver.saveRow("TEST2", List.of(randomValue));
        saver.disconnect(db);

        Map<String, Long> goldPairs = new HashMap<>();
        goldPairs.put("TEST1", 10L);
        goldPairs.put("TEST2", randomValue);

        Column<Long> column = table.getColumn(Long.class, "GOLD");
        Table.Loader<String> loader = Table.loader(table, db);
        Map<String, Long> goldValues = loader.loadColumn(column);
        assertEquals(goldPairs, goldValues);
        loader.disconnect(db);

        db.delete();
    }


    @Test
    public void deleteColumn() {
        Column.Builder uuidColumn = Column.builder(String.class)
                .name("UUID")
                .columnProperties(Column.ColumnProperty.UNIQUE, Column.ColumnProperty.PRIMARY_KEY, Column.ColumnProperty.NOT_NULL);
        Column.Builder goldColumn = Column.builder(Long.class)
                .name("GOLD")
                .columnProperties(Column.ColumnProperty.NOT_NULL)
                .defaultValue(10L);

        Table.Builder playerData = Table.builder(String.class)
                .name("player_data")
                .columnBuilders(uuidColumn, goldColumn);

        Database db = Database.builder()
                .path("test2")
                .addTablesIfNotExists(playerData)
                .build();

        Table table = db.getTable(String.class, playerData.getName());
        Table.Saver<String> saver = Table.saver(table, db);
        saver.saveEntry("TEST1");
        saver.disconnect(db);
        Table.Deleter<String> deleter = Table.deleter(table, db);
        deleter.deleteEntry("TEST1");
        deleter.disconnect(db);
        db.delete();
    }

}
