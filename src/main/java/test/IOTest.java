package test;

import me.vermulst.vermulstutils.data.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IOTest {

    @Test
    public void testSave1() {

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
                .path("test1")
                .addExistingTables()
                .addTablesIfNotExists(playerData)
                .build();

        Table table = db.getTable(String.class, playerData.getName());
        Table.Saver<String> saver = Table.saver(table, db);
        saver.saveEntry("TEST1");
        Long randomValue = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        saver.saveRow("TEST2", List.of(randomValue));
        saver.disconnect(db);

        Table.Loader<String> loader = Table.loader(table, db);
        List<Object> objects = loader.loadRow("TEST1");
        assertEquals(List.of(10L).getFirst(), objects.getFirst());
        List<Object> objects1 = loader.loadRow("TEST2");
        assertEquals(List.of(randomValue).getFirst(), objects1.getFirst());
        loader.disconnect(db);

        db.delete();
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
                .addExistingTables()
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

}
