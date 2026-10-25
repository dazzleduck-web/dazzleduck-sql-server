package io.dazzleduck.sql.otel.collector.config;

import com.typesafe.config.Config;

/**
 * The {@code otel_collector.attribute_types} block: process-wide settings for the attribute type
 * registry, which records which OTLP value types each attribute name has arrived with (see
 * {@code AttributeTypeRegistry}).
 *
 * <p>Whether a stream records anything is <b>not</b> set here. It is set per queue, by the
 * {@code attribute_types_table} parameter of that queue's mapping — unqualified, resolved in the
 * queue's own catalog and schema, exactly like {@code watermark_table}. A queue that does not name
 * one records nothing and pays nothing. So there is no table to configure here, and none could be:
 * queues may share a table or have one each, and with a dynamic registry the collector does not
 * know a queue's schema until a batch arrives.
 *
 * <p>There are two caps, and they are not redundant. {@code maxEntries} is the <b>heap bound</b>:
 * it is sized against memory and stops the process dying. {@code maxEntriesPerStream} is the
 * <b>containment bound</b>: without it, one stream whose attribute names embed ids
 * ({@code user.8f21ac.action}) consumes the whole budget and every other stream silently stops
 * recording new mappings — streams that did nothing wrong lose their description because of a
 * neighbour, and a global counter only says it happened somewhere. The per-stream cap keeps the
 * damage inside the stream causing it, and because {@code otel.attribute_types.dropped} is tagged
 * with the queue, it names that stream.
 *
 * <p>With thousands of streams in a process the per-stream default multiplied by the stream count
 * far exceeds the global cap, so {@code maxEntries} remains the thing that actually protects the
 * heap. A queue may set its own cap with {@code attribute_types_max_entries}, but cannot escape
 * the global one.
 *
 * @param enabled            honour those per-queue tables; false turns the whole registry off
 *                           without editing every queue
 * @param maxEntries         cap on mappings held in memory across every queue — the heap bound
 * @param maxEntriesPerStream default cap on mappings held for any one queue, overridable per queue
 */
public record AttributeTypeSettings(boolean enabled, int maxEntries, int maxEntriesPerStream) {

    public static AttributeTypeSettings defaults() {
        return new AttributeTypeSettings(true, 100_000, 10_000);
    }

    /** Parses the block strictly: a wrong type or an invalid value fails startup. */
    public static AttributeTypeSettings from(Config c) {
        var defaults = defaults();
        var settings = new AttributeTypeSettings(
                !c.hasPath("enabled") || c.getBoolean("enabled"),
                c.hasPath("max_entries") ? c.getInt("max_entries") : defaults.maxEntries(),
                c.hasPath("max_entries_per_stream") ? c.getInt("max_entries_per_stream")
                        : defaults.maxEntriesPerStream());
        if (settings.maxEntries() <= 0) {
            throw new IllegalArgumentException(
                    "attribute_types.max_entries must be positive, got " + settings.maxEntries());
        }
        if (settings.maxEntriesPerStream() <= 0) {
            throw new IllegalArgumentException("attribute_types.max_entries_per_stream must be positive, got "
                    + settings.maxEntriesPerStream());
        }
        return settings;
    }

    /** The cap for one queue: its own if it set one, else the default, never above the heap bound. */
    public int capFor(Integer queueOverride) {
        return Math.min(queueOverride == null ? maxEntriesPerStream : queueOverride, maxEntries);
    }
}
