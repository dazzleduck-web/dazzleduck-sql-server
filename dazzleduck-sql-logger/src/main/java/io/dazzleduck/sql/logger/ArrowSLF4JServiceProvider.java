package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;
import org.slf4j.helpers.NOPMDCAdapter;

public abstract class ArrowSLF4JServiceProvider implements SLF4JServiceProvider {
    private final ArrowSimpleLoggerFactory factory = new ArrowSimpleLoggerFactory();
    @Override public ILoggerFactory getLoggerFactory() { return factory; }
    @Override public MDCAdapter getMDCAdapter() { return new NOPMDCAdapter(); }
    @Override public String getRequestedApiVersion() { return "2.0.0"; }
    @Override public void initialize() {}
}
