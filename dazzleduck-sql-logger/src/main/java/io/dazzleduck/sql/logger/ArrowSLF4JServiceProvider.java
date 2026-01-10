package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

public class ArrowSLF4JServiceProvider implements SLF4JServiceProvider {
    public static final String REQUESTED_API_VERSION = "2.0.99";

    private final ArrowSimpleLoggerFactory loggerFactory = new ArrowSimpleLoggerFactory();
    private final ArrowMDCAdapter mdcAdapter = new ArrowMDCAdapter();
    private final IMarkerFactory markerFactory = new BasicMarkerFactory();

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public String getRequestedApiVersion() {
        return REQUESTED_API_VERSION;
    }

    @Override
    public void initialize() {
        // Perform any initialization needed
        // This is called once by SLF4J
    }
}