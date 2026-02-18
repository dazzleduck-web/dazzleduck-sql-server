package io.dazzleduck.sql.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.Configurator;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.spi.ContextAwareBase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Logback {@link Configurator} that auto-discovers {@code *-logback.xml}
 * files on the classpath and configures a {@link LogForwardingAppender} for each
 * one — no {@code logback.xml} required.
 *
 * <h2>Discovery rules</h2>
 * <ol>
 *   <li><b>Component property set</b> — if the system property
 *       {@code dazzleduck.logback.component} is set (e.g. {@code controller}),
 *       only {@code controller-logback.xml} is loaded.  Use this when multiple
 *       components share the same {@code src/main/resources/} directory:
 *       <pre>  -Ddazzleduck.logback.component=controller</pre></li>
 *   <li><b>No property</b> — every file on the classpath whose name ends with
 *       {@code -logback.xml} is loaded.  Use this when each component has its
 *       own module / classpath.</li>
 * </ol>
 *
 * <h2>Fallback</h2>
 * If no matching file is found this configurator returns
 * {@link ExecutionStatus#INVOKE_NEXT_IF_ANY} so Logback falls through to its
 * normal {@code logback.xml} handling.
 *
 * <p>Registered via SPI:
 * {@code META-INF/services/ch.qos.logback.classic.spi.Configurator}.</p>
 */
public class DazzleDuckLogbackConfigurator extends ContextAwareBase implements Configurator {

    /** System property that pins discovery to a single component config file. */
    public static final String COMPONENT_PROPERTY = "dazzleduck.logback.component";

    private static final String SUFFIX = "-logback.xml";
    private static final String CONSOLE_PATTERN =
            "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n";

    /** Same exclusions as {@link LogForwardingAppender} — prevents infinite loops. */
    private static final String[] EXCLUDED_LOGGER_PREFIXES = {
            "io.dazzleduck.sql.logback.Log",
            "io.dazzleduck.sql.client",
            "org.apache.arrow"
    };

    @Override
    public ExecutionStatus configure(LoggerContext lc) {
        String component = System.getProperty(COMPONENT_PROPERTY);
        List<String> resources = (component != null && !component.trim().isEmpty())
                ? resolveComponent(component.trim())
                : findConfigResources();

        if (resources.isEmpty()) {
            if (component != null) {
                addWarn("DazzleDuckLogbackConfigurator: component '" + component
                        + "' specified but '" + component + SUFFIX
                        + "' was not found on the classpath — "
                        + "falling through to default Logback configuration.");
            } else {
                addInfo("DazzleDuckLogbackConfigurator: no *-logback.xml files found — "
                        + "falling through to default Logback configuration.");
            }
            return ExecutionStatus.INVOKE_NEXT_IF_ANY;
        }

        addInfo("DazzleDuckLogbackConfigurator: configuring from " + resources);

        // Console appender — always present for local output.
        ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> console =
                buildConsoleAppender(lc);

        // Prevent infinite loops: route internal packages to console only.
        for (String prefix : EXCLUDED_LOGGER_PREFIXES) {
            Logger excLogger = lc.getLogger(prefix);
            excLogger.setLevel(Level.INFO);
            excLogger.setAdditive(false);
            excLogger.addAppender(console);
        }

        // Root logger always gets the console appender.
        Logger root = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(console);

        // Build one LogForwardingAppender per discovered file.
        ClassLoader cl = classLoader();
        for (String resource : resources) {
            try (InputStream is = cl.getResourceAsStream(resource)) {
                if (is == null) {
                    addWarn("Could not open classpath resource: " + resource);
                    continue;
                }
                AppenderXmlParser.ParsedAppender parsed = AppenderXmlParser.parse(is);
                LogForwardingAppender appender = buildAppender(lc, parsed);

                // Attach to the logger named in the XML, or root if not specified.
                Logger target = parsed.loggerName
                        .map(lc::getLogger)
                        .orElse(root);
                target.addAppender(appender);

                addInfo("Attached appender '" + parsed.name + "' → logger '"
                        + target.getName() + "'");
            } catch (Exception e) {
                addError("Failed to configure appender from '" + resource + "'", e);
            }
        }

        return ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY;
    }

    // -------------------------------------------------------------------------
    // Classpath scanning
    // -------------------------------------------------------------------------

    /**
     * Returns a single-element list containing {@code <component>-logback.xml}
     * if that resource exists on the classpath, or an empty list if not found.
     * Used when {@link #COMPONENT_PROPERTY} is set.
     */
    private List<String> resolveComponent(String component) {
        String resourceName = component + SUFFIX;
        InputStream probe = classLoader().getResourceAsStream(resourceName);
        if (probe != null) {
            try { probe.close(); } catch (IOException ignored) {}
            return List.of(resourceName);
        }
        return List.of();
    }

    /**
     * Returns the names of all classpath-root resources ending in
     * {@code -logback.xml}, deduplicated and in discovery order.
     */
    private List<String> findConfigResources() {
        Set<String> found = new LinkedHashSet<>();

        // Walk explicit JVM classpath entries (covers typical Maven/Gradle projects).
        String cp = System.getProperty("java.class.path", "");
        if (!cp.isEmpty()) {
            for (String entry : cp.split(File.pathSeparator)) {
                File f = new File(entry);
                if (f.isDirectory()) {
                    scanDirectory(f, found);
                } else if (f.isFile()) {
                    String lower = f.getName().toLowerCase(Locale.ROOT);
                    if (lower.endsWith(".jar") || lower.endsWith(".zip")) {
                        scanJar(f, found);
                    }
                }
            }
        }

        // Also ask the ClassLoader for its roots — covers OSGi / application
        // servers that do not fully expose entries via java.class.path.
        try {
            Enumeration<URL> roots = classLoader().getResources("");
            while (roots.hasMoreElements()) {
                URL root = roots.nextElement();
                if ("file".equals(root.getProtocol())) {
                    scanDirectory(new File(root.toURI()), found);
                }
            }
        } catch (Exception e) {
            addWarn("ClassLoader.getResources() scan failed: " + e.getMessage());
        }

        return new ArrayList<>(found);
    }

    private void scanDirectory(File dir, Set<String> found) {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isFile() && f.getName().endsWith(SUFFIX)) {
                found.add(f.getName());
            }
        }
    }

    private void scanJar(File jarFile, Set<String> found) {
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                // Only classpath-root entries (no sub-directory path separator).
                if (!entry.isDirectory() && !name.contains("/") && name.endsWith(SUFFIX)) {
                    found.add(name);
                }
            }
        } catch (IOException e) {
            // Silently ignore unreadable JARs.
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private LogForwardingAppender buildAppender(LoggerContext lc,
                                                AppenderXmlParser.ParsedAppender parsed) {
        LogForwarderConfig cfg = parsed.config;
        LogForwardingAppender appender = new LogForwardingAppender();
        appender.setName(parsed.name);
        appender.setContext(lc);
        appender.setBaseUrl(cfg.baseUrl());
        appender.setUsername(cfg.username());
        appender.setPassword(cfg.password());
        appender.setIngestionQueue(cfg.ingestionQueue());
        appender.setMinBatchSize(cfg.minBatchSize());
        if (!cfg.project().isEmpty()) {
            appender.setProject(String.join(",", cfg.project()));
        }
        if (!cfg.partitionBy().isEmpty()) {
            appender.setPartitionBy(String.join(",", cfg.partitionBy()));
        }
        appender.start();
        return appender;
    }

    private ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> buildConsoleAppender(
            LoggerContext lc) {
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(lc);
        encoder.setPattern(CONSOLE_PATTERN);
        encoder.start();

        ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ConsoleAppender<>();
        appender.setName("CONSOLE");
        appender.setContext(lc);
        appender.setEncoder(encoder);
        appender.start();
        return appender;
    }

    private ClassLoader classLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return cl != null ? cl : DazzleDuckLogbackConfigurator.class.getClassLoader();
    }
}
