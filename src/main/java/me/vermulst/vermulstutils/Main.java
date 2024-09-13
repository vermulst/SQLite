package me.vermulst.vermulstutils;

import me.vermulst.vermulstutils.data.*;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Set;

public final class Main extends JavaPlugin {

    private static Main main;

    public static final Main getInstance() {
        return main;
    }

    @Override
    public void onEnable() {
        main = this;
        // Plugin startup logic
        Database playerDatabase = new Database("players");
        DatabaseTable databaseTable = new DatabaseTableBuilder("player_money")
                .columns(Set.of(new Column("uuid", ColumnType.STRING, true),
                        new Column("money", ColumnType.LONG, true)))
                .build(playerDatabase.getConnection());
        databaseTable.createOrUpdate();
        playerDatabase.addTable(databaseTable);
    }

    @Override
    public void onDisable() {
        // Plugin shutdown logic
    }
}
