package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class ArrowSimpleLoggerFactory implements ILoggerFactory {
    private final ConcurrentHashMap<String, Logger> map = new ConcurrentHashMap<>();

    @Override
    public Logger getLogger(String name) {
        return map.computeIfAbsent(name, ArrowSimpleLogger::new);
    }
}
