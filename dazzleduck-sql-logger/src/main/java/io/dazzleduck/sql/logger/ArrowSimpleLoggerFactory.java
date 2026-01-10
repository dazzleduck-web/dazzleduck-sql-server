package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating and managing ArrowSimpleLogger instances.
 * Uses a reentrant-safe approach to avoid ConcurrentHashMap.computeIfAbsent
 * recursive update issues when logger creation triggers more logging.
 */
public class ArrowSimpleLoggerFactory implements ILoggerFactory {

    private final ConcurrentHashMap<String, ArrowSimpleLogger> loggerMap = new ConcurrentHashMap<>();

    @Override
    public Logger getLogger(String name) {
        // Check if logger already exists
        ArrowSimpleLogger logger = loggerMap.get(name);
        if (logger != null) {
            return logger;
        }

        // Create new logger outside computeIfAbsent to allow reentrant calls
        // (HttpProducer and other dependencies may trigger additional logging)
        ArrowSimpleLogger newLogger = new ArrowSimpleLogger(name);

        // Use putIfAbsent to handle race conditions
        ArrowSimpleLogger existing = loggerMap.putIfAbsent(name, newLogger);
        if (existing != null) {
            // Another thread created the logger first, close ours and use theirs
            try {
                newLogger.close();
            } catch (Exception e) {
                // Ignore close errors for duplicate logger
            }
            return existing;
        }

        return newLogger;
    }

    /**
     * Close all loggers and flush any pending logs
     * Should be called during application shutdown
     */
    public void closeAll() {
        loggerMap.values().forEach(logger -> {
            try {
                logger.close();
            } catch (Exception e) {
                System.err.println("[ArrowSimpleLoggerFactory] Failed to close logger: " + logger.getName());
                e.printStackTrace(System.err);
            }
        });
    }

    /**
     * Get count of active loggers
     */
    public int getLoggerCount() {
        return loggerMap.size();
    }

    /**
     * Remove a logger by name
     * Closes the logger before removing
     */
    public void removeLogger(String name) {
        ArrowSimpleLogger logger = loggerMap.remove(name);
        if (logger != null) {
            try {
                logger.close();
            } catch (Exception e) {
                System.err.println("[ArrowSimpleLoggerFactory] Failed to close logger during removal: " + name);
                e.printStackTrace(System.err);
            }
        }
    }
}
