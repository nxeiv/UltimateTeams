package dev.xf3d3.ultimateteams.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.tcoded.folialib.wrapper.task.WrappedTask;
import dev.xf3d3.ultimateteams.UltimateTeams;
import dev.xf3d3.ultimateteams.models.Team;
import dev.xf3d3.ultimateteams.models.TeamEnderChest;
import dev.xf3d3.ultimateteams.models.TeamEnderChestBackup;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;

/**
 * Manages automatic backups of team ender chests.
 */
public class EnderChestBackupManager {
    private static final long INITIAL_RETRY_TICKS = 20L;
    private static final int BACKUP_INTERVAL_MINUTES = 30;
    private static final long BACKUP_INTERVAL_TICKS = BACKUP_INTERVAL_MINUTES * 60L * 20L;
    private static final int MAX_BACKUPS_PER_CHEST = 10;

    private final UltimateTeams plugin;
    private final Path backupsPath;
    private final Gson gson;
    private final Map<String, List<TeamEnderChestBackup>> backups = new HashMap<>();
    private final Object backupsLock = new Object();
    private final Object saveLock = new Object();

    private volatile WrappedTask scheduledTask;
    private volatile boolean running = true;

    public EnderChestBackupManager(@NotNull UltimateTeams plugin) {
        this.plugin = plugin;
        this.backupsPath = plugin.getDataFolder().toPath().resolve("echest_backups.json");
        this.gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .setPrettyPrinting()
                .create();

        loadBackups();
        startAutoBackup();
    }

    private void startAutoBackup() {
        running = true;
        // Team data is loaded asynchronously during plugin startup. Do not
        // create the first backup until TeamsStorage has finished loading.
        scheduleNextBackup(INITIAL_RETRY_TICKS);
    }

    private void scheduleNextBackup(long delayTicks) {
        if (!running) {
            return;
        }

        scheduledTask = plugin.getScheduler().runLater(() -> {
            scheduledTask = null;
            if (!running) {
                return;
            }

            if (!plugin.isLoaded()) {
                scheduleNextBackup(INITIAL_RETRY_TICKS);
                return;
            }

            plugin.runAsync(task -> {
                if (!running) {
                    return;
                }

                try {
                    if (plugin.getSettings().getGeneral().isEchestBackupLog()) {
                        plugin.getLogger().info("Creating automatic ender chest backups...");
                    }

                    backupAllChests();
                    saveBackups();

                    if (plugin.getSettings().getGeneral().isEchestBackupLog()) {
                        plugin.getLogger().info("Automatic ender chest backups completed.");
                    }
                } catch (Exception exception) {
                    plugin.getLogger().log(Level.SEVERE, "Error during automatic ender chest backup.", exception);
                } finally {
                    if (running) {
                        scheduleNextBackup(BACKUP_INTERVAL_TICKS);
                    }
                }
            });
        }, delayTicks);
    }

    /**
     * Stop future backups and persist the last completed snapshot.
     */
    public void shutdown() {
        running = false;

        WrappedTask task = scheduledTask;
        scheduledTask = null;
        if (task != null) {
            task.cancel();
        }

        saveBackups();
    }

    /**
     * Back up every team's ender chests.
     */
    public void backupAllChests() {
        List<Team> teamSnapshot = new ArrayList<>(plugin.getTeamStorageUtil().getTeams());
        for (Team team : teamSnapshot) {
            List<TeamEnderChest> chestSnapshot = new ArrayList<>(team.getEnderChests().values());
            for (TeamEnderChest chest : chestSnapshot) {
                backupChest(team.getId(), chest);
            }
        }
    }

    /**
     * Back up all chests for a specific team.
     *
     * @param teamId the team ID
     * @return number of chests backed up
     */
    public int backupTeamChests(int teamId) {
        Optional<Team> teamOptional = plugin.getTeamStorageUtil().findTeam(teamId);
        if (teamOptional.isEmpty()) {
            return 0;
        }

        Team team = teamOptional.get();
        List<TeamEnderChest> chestSnapshot = new ArrayList<>(team.getEnderChests().values());
        for (TeamEnderChest chest : chestSnapshot) {
            backupChest(team.getId(), chest);
        }
        return chestSnapshot.size();
    }

    /**
     * Back up one ender chest.
     */
    public void backupChest(int teamId, @NotNull TeamEnderChest chest) {
        String key = getKey(teamId, chest.getChestNumber());
        TeamEnderChestBackup backup = TeamEnderChestBackup.fromChest(teamId, chest);

        synchronized (backupsLock) {
            List<TeamEnderChestBackup> chestBackups = backups.computeIfAbsent(key, ignored -> new ArrayList<>());
            chestBackups.add(backup);
            chestBackups.sort(Comparator.comparingLong(TeamEnderChestBackup::getTimestamp));

            if (chestBackups.size() > MAX_BACKUPS_PER_CHEST) {
                chestBackups.subList(0, chestBackups.size() - MAX_BACKUPS_PER_CHEST).clear();
            }
        }
    }

    /**
     * Get all backups for a chest, newest first.
     */
    @NotNull
    public List<TeamEnderChestBackup> getBackups(int teamId, int chestNumber) {
        List<TeamEnderChestBackup> result;
        synchronized (backupsLock) {
            result = new ArrayList<>(backups.getOrDefault(getKey(teamId, chestNumber), List.of()));
        }
        result.sort(Comparator.comparingLong(TeamEnderChestBackup::getTimestamp).reversed());
        return result;
    }

    /**
     * Get a backup by index (zero is the newest).
     */
    @NotNull
    public Optional<TeamEnderChestBackup> getBackup(int teamId, int chestNumber, int index) {
        List<TeamEnderChestBackup> chestBackups = getBackups(teamId, chestNumber);
        return index >= 0 && index < chestBackups.size()
                ? Optional.of(chestBackups.get(index))
                : Optional.empty();
    }

    /**
     * Restore a chest from a backup.
     */
    public boolean restoreBackup(int teamId, int chestNumber, int backupIndex) {
        Optional<Team> teamOptional = plugin.getTeamStorageUtil().findTeam(teamId);
        if (teamOptional.isEmpty()) {
            return false;
        }

        Team team = teamOptional.get();
        Optional<TeamEnderChest> chestOptional = team.getEnderChest(chestNumber);
        Optional<TeamEnderChestBackup> backupOptional = getBackup(teamId, chestNumber, backupIndex);
        if (chestOptional.isEmpty() || backupOptional.isEmpty()) {
            return false;
        }

        TeamEnderChest chest = chestOptional.get();
        TeamEnderChestBackup backup = backupOptional.get();
        chest.setSerializedContents(backup.getSerializedContents());
        chest.setRows(backup.getRows());
        plugin.getDatabase().updateTeam(team);
        return true;
    }

    public void deleteBackups(int teamId, int chestNumber) {
        synchronized (backupsLock) {
            backups.remove(getKey(teamId, chestNumber));
        }
        saveBackups();
    }

    public void deleteTeamBackups(int teamId) {
        String prefix = teamId + "-";
        synchronized (backupsLock) {
            backups.keySet().removeIf(key -> key.startsWith(prefix));
        }
        saveBackups();
    }

    private String getKey(int teamId, int chestNumber) {
        return teamId + "-" + chestNumber;
    }

    private void loadBackups() {
        if (!Files.exists(backupsPath)) {
            return;
        }

        Type type = new TypeToken<Map<String, List<TeamEnderChestBackup>>>() { }.getType();
        try (var reader = Files.newBufferedReader(backupsPath, StandardCharsets.UTF_8)) {
            Map<String, List<TeamEnderChestBackup>> loaded = gson.fromJson(reader, type);
            if (loaded == null) {
                return;
            }

            synchronized (backupsLock) {
                loaded.forEach((key, value) -> {
                    if (key != null && value != null) {
                        backups.put(key, new ArrayList<>(value));
                    }
                });
            }

            plugin.getLogger().log(Level.INFO, "Loaded {0} ender chest backup entries.", loaded.size());
        } catch (Exception exception) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load ender chest backups. The original file was left untouched.", exception);
        }
    }

    /**
     * Save a consistent snapshot by writing a temporary file and then replacing
     * the previous backup file. A partial write will not truncate the last good file.
     */
    public void saveBackups() {
        synchronized (saveLock) {
            // Take the snapshot after acquiring the write lock so an older
            // snapshot cannot overwrite a newer save that finished first.
            Map<String, List<TeamEnderChestBackup>> snapshot = new LinkedHashMap<>();
            synchronized (backupsLock) {
                backups.forEach((key, value) -> snapshot.put(key, new ArrayList<>(value)));
            }

            Path temporaryPath = backupsPath.resolveSibling(backupsPath.getFileName() + ".tmp");
            try {
                Files.createDirectories(backupsPath.getParent());
                Files.writeString(
                        temporaryPath,
                        gson.toJson(snapshot),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE
                );

                try {
                    Files.move(temporaryPath, backupsPath,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException exception) {
                    Files.move(temporaryPath, backupsPath, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException exception) {
                plugin.getLogger().log(Level.SEVERE, "Failed to save ender chest backups.", exception);
                try {
                    Files.deleteIfExists(temporaryPath);
                } catch (IOException cleanupException) {
                    plugin.getLogger().log(Level.WARNING, "Could not remove the temporary ender chest backup file.", cleanupException);
                }
            }
        }
    }
}
