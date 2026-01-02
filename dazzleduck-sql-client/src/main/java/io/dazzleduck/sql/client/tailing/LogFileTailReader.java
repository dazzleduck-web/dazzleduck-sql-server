package io.dazzleduck.sql.client.tailing;

import io.dazzleduck.sql.client.tailing.model.LogFileWithLines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Monitors a directory for log files and reads new lines from all matching files.
 * Tracks byte position for each file individually and detects new files.
 */
public final class LogFileTailReader {

    private static final Logger logger = LoggerFactory.getLogger(LogFileTailReader.class);

    private final Path logDirectory;
    private final PathMatcher fileMatcher;
    // Track last read position for each file
    private final Map<Path, Long> filePositions;
    // Track known files to detect new ones
    private final Set<Path> knownFiles;

    /**
     * Create a reader that monitors a directory for log files
     * @param directoryPath Path to the directory containing log files
     * @param filePattern Glob pattern to match files (e.g., "*.log", "app*.json")
     */
    public LogFileTailReader(String directoryPath, String filePattern) {
        this.logDirectory = Paths.get(directoryPath);
        this.fileMatcher = FileSystems.getDefault().getPathMatcher("glob:" + filePattern);
        this.filePositions = new HashMap<>();
        this.knownFiles = new HashSet<>();

        if (!Files.exists(logDirectory)) {
            logger.warn("Log directory does not exist: {}", logDirectory);
        } else if (!Files.isDirectory(logDirectory)) {
            throw new IllegalArgumentException("Path is not a directory: " + logDirectory);
        }
        logger.info("Initialized LogFileTailReader: directory={}, pattern={}", directoryPath, filePattern);
    }

    /**
     * Reads new lines from all matching log files in the directory.
     * Returns a list of LogFileWithLines objects, each containing the file name and its new lines.
     */
    public List<LogFileWithLines> readNewLinesGroupedByFile() throws IOException {
        List<LogFileWithLines> result = new ArrayList<>();

        if (!Files.exists(logDirectory)) {
            logger.debug("Log directory not found: {}", logDirectory);
            return result;
        }
        // Get all matching files in directory
        Set<Path> currentFiles = getMatchingFiles();
        // Detect new files
        Set<Path> newFiles = new HashSet<>(currentFiles);
        newFiles.removeAll(knownFiles);

        if (!newFiles.isEmpty()) {
            logger.info("Detected {} new log file(s): {}", newFiles.size(), newFiles.stream().map(Path::getFileName).collect(Collectors.toList()));
        }

        // Update known files
        knownFiles.addAll(currentFiles);

        // Remove files that no longer exist
        Set<Path> removedFiles = new HashSet<>(filePositions.keySet());
        removedFiles.removeAll(currentFiles);
        for (Path removed : removedFiles) {
            filePositions.remove(removed);
            knownFiles.remove(removed);
            logger.info("File removed from monitoring: {}", removed.getFileName());
        }

        // Read from each file
        for (Path filePath : currentFiles) {
            try {
                List<String> newLines = readNewLinesFromFile(filePath);
                if (!newLines.isEmpty()) {
                    result.add(new LogFileWithLines(filePath.getFileName().toString(), newLines));
                }
            } catch (IOException e) {
                logger.error("Error reading file {}: {}", filePath.getFileName(), e.getMessage());
            }
        }

        return result;
    }

    private Set<Path> getMatchingFiles() throws IOException {
        if (!Files.exists(logDirectory)) {
            return Collections.emptySet();
        }

        try (Stream<Path> paths = Files.list(logDirectory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> fileMatcher.matches(path.getFileName()))
                    .collect(Collectors.toSet());
        }
    }

    private List<String> readNewLinesFromFile(Path filePath) throws IOException {
        List<String> newLines = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            long currentFileLength = raf.length();
            long lastPosition = filePositions.getOrDefault(filePath, 0L);

            // Handle file truncation or rotation
            if (currentFileLength < lastPosition) {
                logger.warn("File {} truncated or rotated. Resetting position from {} to 0", filePath.getFileName(), lastPosition);
                lastPosition = 0;
            }

            // Read only if there's new data
            if (currentFileLength > lastPosition) {
                long bytesToRead = currentFileLength - lastPosition;
                logger.debug("Reading file {}: bytes {} to {} ({} bytes)", filePath.getFileName(), lastPosition, currentFileLength, bytesToRead);

                raf.seek(lastPosition);

                String line;
                while ((line = raf.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        newLines.add(line);
                    }
                }

                filePositions.put(filePath, raf.getFilePointer());
            }
        }
        return newLines;
    }

    /**
     * Get current read positions for all monitored files
     * @return Map of filename to byte position
     */
    public Map<String, Long> getFilePositions() {
        Map<String, Long> positions = new HashMap<>();
        filePositions.forEach((path, pos) -> positions.put(path.getFileName().toString(), pos));
        return positions;
    }

    /**
     * Get count of files currently being monitored
     */
    public int getMonitoredFileCount() {
        return knownFiles.size();
    }

    /**
     * Reset all file positions to 0
     */
    public void reset() {
        filePositions.clear();
        logger.info("All file positions reset to 0");
    }
}