package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

public final class ArrowSLF4JServiceProvider
        implements SLF4JServiceProvider {

    private final ArrowSimpleLoggerFactory loggerFactory =
            new ArrowSimpleLoggerFactory();

    private final IMarkerFactory markerFactory =
            new BasicMarkerFactory();

    private final MDCAdapter mdcAdapter =
            new NOPMDCAdapter();

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public String getRequestedApiVersion() {
        return "2.0.0";
    }

    @Override
    public void initialize() {
        // Optional: add shutdown hook if you want guaranteed flush
        Runtime.getRuntime().addShutdownHook(new Thread(loggerFactory::closeAll));
    }
}
