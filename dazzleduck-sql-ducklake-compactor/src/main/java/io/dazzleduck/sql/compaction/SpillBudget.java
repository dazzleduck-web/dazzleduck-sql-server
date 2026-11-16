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
 * Kubernetes, where an {@code emptyDir}'s {@code sizeLimit} is not visible through statfs.
 *
 * <p>An instance whose own settings set {@code temp_directory} keeps it, and gets no size either
 * unless they set it.
 */
final class SpillBudget {

    private static final Logger logger = LoggerFactory.getLogger(SpillBudget.class);

    static final String DEFAULT_DIRECTORY = "/tmp/ducklake-compactor";
    /** No instance is given less. */
    static final long MINIMUM_BYTES = 256L << 20;
    /** /health alerts when the instances together use more than this share of the budget ... */
    static final double USED_ALERT_SHARE = 0.8;
    /** ... or when the free space under the root falls below this share of it. */
    static final double FREE_ALERT_SHARE = 0.1;

    /** No spill directory: DuckDB's defaults. */
    static final SpillBudget NONE = new SpillBudget(null, -1);

    private static final Pattern SETS_DIRECTORY = Pattern.compile("(?i)\\btemp_directory\\b");
    private static final Pattern SETS_SIZE = Pattern.compile("(?i)\\bmax_temp_directory_size\\b");
    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

    private final Path root;
    private final long budgetBytes;

    SpillBudget(Path root, long budgetBytes) {
        this.root = root;
        this.budgetBytes = budgetBytes;
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
        long budget = free > 0 && maxBytes > 0 ? Math.min(free, maxBytes) : free > 0 ? free : maxBytes;
        logger.info("DuckDB instances spill under {}, a budget of {} bytes ({} free{})", root, budget, free,
                maxBytes > 0 ? ", capped at " + maxBytes : "");
        return new SpillBudget(root, budget);
    }

    Path root() {
        return root;
    }

    long budgetBytes() {
        return budgetBytes;
    }

    /** The {@code max_temp_directory_size} for {@code share} of the budget, or -1 when unknown. */
    long limit(double share, int databases) {
        if (budgetBytes <= 0) {
            return -1;
        }
        return Math.max(MINIMUM_BYTES, (long) (share * budgetBytes / Math.max(1, databases)));
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
            Files.delete(Files.createTempFile(dir, "probe", ".tmp"));
        } catch (IOException | RuntimeException e) {
            throw new SQLException("Spill directory %s is not writable: %s".formatted(dir, e), e);
        }
        List<String> with = new ArrayList<>(settings.size() + 2);
        with.add("SET temp_directory = '" + dir.toString().replace("'", "''") + "'");
        long limit = limit(share, databases);
        if (limit > 0 && settings.stream().noneMatch(s -> SETS_SIZE.matcher(s).find())) {
            with.add("SET max_temp_directory_size = '" + limit + " bytes'");
        }
        with.addAll(settings);
        return List.copyOf(with);
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
        return new CompactionStats.SpillStatus(root.toString(), budgetBytes, used, free);
    }

    /** The alerts for {@link #status()}: spill near its budget, or the disk near full. */
    static List<String> alerts(CompactionStats.SpillStatus status) {
        List<String> alerts = new ArrayList<>();
        if (status == null || status.budgetBytes() <= 0) {
            return alerts;
        }
        if (status.usedBytes() > USED_ALERT_SHARE * status.budgetBytes()) {
            alerts.add("Spill under %s uses %d bytes, above %.0f%% of its budget of %d bytes".formatted(
                    status.directory(), status.usedBytes(), USED_ALERT_SHARE * 100, status.budgetBytes()));
        }
        if (status.freeBytes() >= 0 && status.freeBytes() < FREE_ALERT_SHARE * status.budgetBytes()) {
            alerts.add("Free space under %s is %d bytes, below %.0f%% of the spill budget of %d bytes".formatted(
                    status.directory(), status.freeBytes(), FREE_ALERT_SHARE * 100, status.budgetBytes()));
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
