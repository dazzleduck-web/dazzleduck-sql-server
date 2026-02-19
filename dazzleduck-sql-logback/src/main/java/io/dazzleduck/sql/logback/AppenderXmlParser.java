package io.dazzleduck.sql.logback;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.*;

/**
 * Utility for parsing {@link LogForwardingAppender} configuration from XML.
 * Extracted as shared utility so both {@link LogForwardingAppender} and
 * {@link DazzleDuckLogbackConfigurator} can reuse the same logic.
 */
final class AppenderXmlParser {

    private AppenderXmlParser() {}

    /** Parsed result containing all configuration needed for an appender. */
    static final class ParsedAppender {
        final String name;
        final LogForwarderConfig config;
        final Optional<String> loggerName;

        ParsedAppender(String name, LogForwarderConfig config, Optional<String> loggerName) {
            this.name = name;
            this.config = config;
            this.loggerName = loggerName;
        }
    }

    /**
     * Parse a {@link LogForwardingAppender} from an XML file.
     * The file may be a full Logback {@code <configuration>} document (the first
     * {@code LogForwardingAppender} element is used) or a bare {@code <appender>}
     * element. The XML property names match Logback setter names exactly,
     * so the file looks identical to the appender block in logback.xml.
     *
     * <p>Supported root elements:</p>
     * <ul>
     *   <li>{@code <configuration>} with a child {@code <appender class="...LogForwardingAppender">}
     *   <li>{@code <appender class="...LogForwardingAppender">} directly
     * </ul>
     *
     * <p>Optional {@code <logger>} element inside the appender can specify
     * which logger to attach to (otherwise attaches to root).</p>
     */
    static ParsedAppender parse(InputStream is) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(is);
        doc.getDocumentElement().normalize();

        Element root = doc.getDocumentElement();
        Element appenderEl;

        if ("configuration".equals(root.getTagName())) {
            NodeList appenders = root.getElementsByTagName("appender");
            appenderEl = null;
            for (int i = 0; i < appenders.getLength(); i++) {
                Element el = (Element) appenders.item(i);
                if (el.getAttribute("class").endsWith("LogForwardingAppender")) {
                    appenderEl = el;
                    break;
                }
            }
            if (appenderEl == null) {
                throw new IllegalArgumentException(
                        "No LogForwardingAppender element found in XML configuration");
            }
        } else if ("appender".equals(root.getTagName())) {
            appenderEl = root;
        } else {
            throw new IllegalArgumentException(
                    "Unexpected root element <" + root.getTagName() + "> in XML configuration. " +
                    "Expected <configuration> or <appender>.");
        }

        return parseAppenderElement(appenderEl);
    }

    private static ParsedAppender parseAppenderElement(Element el) {
        String name        = el.getAttribute("name");
        String logger     = childText(el, "logger", null);

        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl(childText(el, "baseUrl", "http://localhost:8081"))
                .username(childText(el, "username", "admin"))
                .password(childText(el, "password", "admin"))
                .ingestionQueue(childText(el, "ingestionQueue", "log"))
                .minBatchSize(Long.parseLong(childText(el, "minBatchSize", "1024")))
                .project(splitList(childText(el, "project", "")))
                .partitionBy(splitList(childText(el, "partitionBy", "")))
                .build();

        return new ParsedAppender(name, config, Optional.ofNullable(logger));
    }

    private static List<String> splitList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    private static String childText(Element parent, String tagName, String defaultValue) {
        NodeList nodes = parent.getElementsByTagName(tagName);
        if (nodes.getLength() > 0) {
            String text = nodes.item(0).getTextContent();
            if (text != null && !text.trim().isEmpty()) {
                return text.trim();
            }
        }
        return defaultValue;
    }
}
