package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.util.concurrent.ConcurrentHashMap;

public final class ArrowSimpleLoggerFactory implements ILoggerFactory {

    private final ConcurrentHashMap<String, Logger> map = new ConcurrentHashMap<>();

    private static final String[] INTERNAL_PREFIXES = {
            "org.apache.arrow",
            "io.netty",
            "io.grpc",
            "io.dazzleduck.sql.common.ingestion",
            "io.dazzleduck.sql.client.HttpSender"
    };

    @Override
    public Logger getLogger(String name) {
        if (name == null) {
            return NOPLogger.NOP_LOGGER;
        }

        for (String prefix : INTERNAL_PREFIXES) {
            if (name.startsWith(prefix)) {
                return NOPLogger.NOP_LOGGER;
            }
        }

        return map.computeIfAbsent(name, ArrowSimpleLogger::new);
    }

    public void closeAll() {
        map.values().forEach(logger -> {
            if (logger instanceof ArrowSimpleLogger asl) {
                try {
                    asl.close();
                } catch (Exception ignored) {

                }
            }
        });
    }
}
