package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Where the compactor's DuckDB instances spill, and how much each may. DuckDB's default
 * {@code temp_directory} for an in-memory database is {@code .tmp} in the working directory, and it
 * creates only the last level of a configured one; both fail late, only when a merge needs to spill
 * (a read-only root filesystem, a missing parent). So each instance gets its own subdirectory of
 * one root, {@code <root>/<database>/<kind>}, created and probed (a file written and deleted) when
 * the instance opens: a tier whose directory is not writable does not start. Instances do not share
 * a directory, since DuckDB cleans up its temp directory when the instance closes.
 *
 * <p>{@code max_temp_directory_size} is split like {@link MemoryBudget}, from a budget of the free
 * space under the root at startup, or a configured cap when that is smaller. The cap matters on
 * Kubernetes, where an {@code emptyDir}'s {@code sizeLimit} is not visible through statfs and going
 * over it evicts the pod, so under a cap no instance is raised to {@link #MINIMUM_BYTES}: the
 * instances' sizes always add up to less than the cap.
 *
 * <p>A process killed while spilling (an out-of-memory kill) leaves its spill files behind, and
 * DuckDB removes them only when an instance spills to the same directory again; an {@code emptyDir}
 * keeps them across container restarts. So the first time this process uses an instance's
 * directory, it deletes any {@code duckdb_temp_*} files left in it.
 *
 * <p>An instance whose own settings set {@code temp_directory} keeps it, and gets no size either
 * unless they set it.
 */
final class SpillBudget {

    private static final Logger logger = LoggerFactory.getLogger(SpillBudget.class);

    static final String DEFAULT_DIRECTORY = "/tmp/ducklake-compactor";
    /** No instance is given less, unless the budget is a configured cap. */
    static final long MINIMUM_BYTES = 256L << 20;
    /**
     * The most of the budget the instances' sizes add up to. The shares are MemoryBudget's, which
     * divide what the JVM leaves and add up to nearly all of it; a disk has other users (logs, the
     * rest of /tmp), so spill keeps to this part of the free space, as it did before the shares grew.
     */
    static final double BUDGET_SHARE = 0.68;
    /**
     * /health alerts when the instances together use more than this share of the sizes they were
     * given, or when the free space under the root is less than what they may still spill.
     */
    static final double USED_ALERT_SHARE = 0.8;
    /** What DuckDB names its spill files. */
    static final String SPILL_FILE_PREFIX = "duckdb_temp_";

    /** No spill directory: DuckDB's defaults. */
    static final SpillBudget NONE = new SpillBudget(null, -1, false);

    private static final Pattern SETS_DIRECTORY = Pattern.compile("(?i)\\btemp_directory\\b");
    private static final Pattern SETS_SIZE = Pattern.compile("(?i)\\bmax_temp_directory_size\\b");
    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

    private final Path root;
    private final long budgetBytes;
    private final boolean capped;
    /** The {@code max_temp_directory_size} given to each instance's directory. */
    private final java.util.concurrent.ConcurrentHashMap<Path, Long> allocated = new java.util.concurrent.ConcurrentHashMap<>();
    /** Directories already cleared of an earlier process's spill files. */
    private final java.util.Set<Path> cleared = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** A budget of the free space: no instance gets less than {@link #MINIMUM_BYTES}. */
    SpillBudget(Path root, long budgetBytes) {
        this(root, budgetBytes, false);
    }

    /** @param capped whether the budget is a configured cap that the instances' sizes must stay under */
    SpillBudget(Path root, long budgetBytes, boolean capped) {
        this.root = root;
        this.budgetBytes = budgetBytes;
        this.capped = capped;
    }

    /**
     * @param directory the root, or null or blank for DuckDB's defaults
     * @param maxBytes  a cap on the budget, or 0 or less for the free space alone
     */
    static SpillBudget create(String directory, long maxBytes) {
        if (directory == null || directory.isBlank()) {
            return NONE;
        }
        Path root = Path.of(directory).toAbsolutePath();
        long free = -1;
        try {
            Files.createDirectories(root);
            free = Files.getFileStore(root).getUsableSpace();
        } catch (IOException e) {
            // Each instance probes its own directory when it opens and reports what is wrong
            logger.error("Spill directory {} cannot be created: {}", root, e.toString());
        }
        boolean capped = maxBytes > 0 && (free <= 0 || maxBytes <= free);
        long budget = capped ? maxBytes : free;
        logger.info("DuckDB instances spill under {}, a budget of {} bytes ({} free{})", root, budget, free,
                maxBytes > 0 ? ", capped at " + maxBytes : "");
        return new SpillBudget(root, budget, capped);
    }

    Path root() {
        return root;
    }

    long budgetBytes() {
        return budgetBytes;
    }

    /**
     * The {@code max_temp_directory_size} for {@code share} of the budget, or -1 when unknown. At
     * least {@link #MINIMUM_BYTES}, except under a configured cap, which the sizes must not exceed.
     */
    long limit(double share, int databases) {
        if (budgetBytes <= 0) {
            return -1;
        }
        long split = (long) (BUDGET_SHARE * share * budgetBytes / Math.max(1, databases));
        return capped ? Math.max(1, split) : Math.max(MINIMUM_BYTES, split);
    }

    /**
     * The settings with {@code SET temp_directory} and {@code SET max_temp_directory_size} for one
     * instance first, its directory created and probed; unchanged without a root or when they set
     * {@code temp_directory} themselves.
     *
     * @param kind the instance: a tier's name, or housekeeping, retention or file counts
     * @throws SQLException when the directory cannot be created or written
     */
    List<String> apply(String database, String kind, double share, int databases, List<String> settings)
            throws SQLException {
        if (root == null || settings.stream().anyMatch(s -> SETS_DIRECTORY.matcher(s).find())) {
            return settings;
        }
        Path dir = directory(database, kind);
        try {
            Files.createDirectories(dir);
            if (cleared.add(dir)) {
                clearLeftovers(dir);
            }
            Files.delete(Files.createTempFile(dir, "probe", ".tmp"));
        } catch (IOException | RuntimeException e) {
            throw new SQLException("Spill directory %s is not writable: %s".formatted(dir, e), e);
        }
        List<String> with = new ArrayList<>(settings.size() + 2);
        with.add("SET temp_directory = '" + dir.toString().replace("'", "''") + "'");
        long limit = limit(share, databases);
        if (limit > 0 && settings.stream().noneMatch(s -> SETS_SIZE.matcher(s).find())) {
            with.add("SET max_temp_directory_size = '" + limit + " bytes'");
            allocated.put(dir, limit);
        }
        with.addAll(settings);
        return List.copyOf(with);
    }

    /** Deletes the spill files an earlier process left in {@code dir}; no instance uses it yet. */
    private static void clearLeftovers(Path dir) throws IOException {
        long files = 0;
        long bytes = 0;
        try (var entries = Files.newDirectoryStream(dir, SPILL_FILE_PREFIX + "*")) {
            for (Path file : entries) {
                if (Files.isRegularFile(file)) {
                    bytes += Files.size(file);
                    Files.delete(file);
                    files++;
                }
            }
        }
        if (files > 0) {
            logger.info("Deleted {} spill file(s), {} bytes, that an earlier process left in {}", files, bytes, dir);
        }
    }

    Path directory(String database, String kind) {
        return root.resolve(UNSAFE.matcher(database).replaceAll("_")).resolve(UNSAFE.matcher(kind).replaceAll("_"));
    }

    /**
     * What {@code /health} reports: the bytes spilled under the root now, the free space, and the
     * budget; null without a root.
     */
    CompactionStats.SpillStatus status() {
        if (root == null) {
            return null;
        }
        long used = -1;
        long free = -1;
        try {
            free = Files.getFileStore(root).getUsableSpace();
            AtomicLong sum = new AtomicLong();
            Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
                @Override
                public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    sum.addAndGet(attrs.size());
                    return java.nio.file.FileVisitResult.CONTINUE;
                }

                @Override
                public java.nio.file.FileVisitResult visitFileFailed(Path file, IOException e) {
                    // a spill file deleted while walking
                    return java.nio.file.FileVisitResult.CONTINUE;
                }
            });
            used = sum.get();
        } catch (IOException e) {
            logger.debug("Cannot read the spill directory {}: {}", root, e.toString());
        }
        long given = allocated.values().stream().mapToLong(Long::longValue).sum();
        return new CompactionStats.SpillStatus(root.toString(), budgetBytes, given, used, free);
    }

    /**
     * The alerts for {@link #status()}: the instances near the sizes they were given (they fail a
     * merge that needs more), or less free space than they may still spill (the disk fills first).
     */
    static List<String> alerts(CompactionStats.SpillStatus status) {
        List<String> alerts = new ArrayList<>();
        if (status == null || status.allocatedBytes() <= 0 || status.usedBytes() < 0) {
            return alerts;
        }
        if (status.usedBytes() > USED_ALERT_SHARE * status.allocatedBytes()) {
            alerts.add("Spill under %s uses %d bytes, above %.0f%% of the %d bytes its instances may spill".formatted(
                    status.directory(), status.usedBytes(), USED_ALERT_SHARE * 100, status.allocatedBytes()));
        }
        long headroom = status.allocatedBytes() - status.usedBytes();
        if (status.freeBytes() >= 0 && status.freeBytes() < headroom) {
            alerts.add(("Free space under %s is %d bytes, less than the %d bytes its instances may still spill")
                    .formatted(status.directory(), status.freeBytes(), headroom));
        }
        return alerts;
    }

    /** One kind of instance: its directory name and share, applied per database when it opens. */
    record Slot(SpillBudget budget, String kind, double share, int databases) {

        static final Slot NONE = new Slot(SpillBudget.NONE, "none", 0, 1);

        List<String> apply(String database, List<String> settings) throws SQLException {
            return budget.apply(database, kind, share, databases, settings);
        }
    }
}
