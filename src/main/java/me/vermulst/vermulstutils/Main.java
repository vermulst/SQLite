package me.vermulst.vermulstutils;

import me.vermulst.vermulstutils.data.*;
import me.vermulst.vermulstutils.data.column.ColumnBuilder;
import me.vermulst.vermulstutils.data.column.ColumnProperty;
import org.bukkit.plugin.java.JavaPlugin;

public final class Main extends JavaPlugin {

    private static Main main;

    public static final Main getInstance() {
        return main;
    }

    @Override
    public void onEnable() {
        main = this;
        // Plugin startup logic

        ColumnBuilder uuidColumn = new ColumnBuilder<>(String.class)
                .columnProperties(ColumnProperty.PRIMARY_KEY, ColumnProperty.UNIQUE, ColumnProperty.NOT_NULL);

        ColumnBuilder moneyColumn = new ColumnBuilder<>(Long.class)
                .columnProperties(ColumnProperty.PRIMARY_KEY, ColumnProperty.UNIQUE, ColumnProperty.NOT_NULL)
                .defaultValue(10L);

        TableBuilder moneyTable = new TableBuilder()
                .name("player_money")
                .columnBuilders(uuidColumn, moneyColumn);

        DatabaseBuilder databaseBuilder = new DatabaseBuilder()
                .path("players")
                .tables(moneyTable);

        Database database = databaseBuilder.build();
    }

    @Override
    public void onDisable() {
        // Plugin shutdown logic
    }
}
