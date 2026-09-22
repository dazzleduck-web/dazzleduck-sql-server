package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;

import java.util.Map;

/**
 * Fills the trailing claims map column with the caller's verified JWT claims — the same map
 * on every row, since one export request has exactly one token. Runs after the per-signal
 * batch writer, which only touches the base columns.
 */
final class ClaimsColumnWriter {

    private ClaimsColumnWriter() {}

    static void write(VectorSchemaRoot root, Map<String, String> claims) {
        int rowCount = root.getRowCount();
        MapVector claimsVec = (MapVector) root.getVector(IngestionHandler.CLAIMS_COLUMN);

        // Single entrySet pass: encodes once per batch and keeps key/value pairing guaranteed.
        // This column is the extreme case of the duplication MapColumnWriter exists to make
        // cheap — one export request carries exactly one token, so the SAME map is written on
        // every one of the batch's rows. Encoding once and replaying the bytes means the
        // per-row cost is just the Arrow offset bookkeeping.
        byte[][] keys = new byte[claims.size()][];
        byte[][] values = new byte[claims.size()][];
        int n = 0;
        for (Map.Entry<String, String> claim : claims.entrySet()) {
            keys[n] = MapColumnWriter.utf8(claim.getKey());
            // A null claim value used to throw from Text's constructor; write a null entry
            // instead, matching how the attribute maps treat an absent value.
            values[n] = claim.getValue() == null ? null : MapColumnWriter.utf8(claim.getValue());
            n++;
        }

        MapColumnWriter writer = MapColumnWriter.of(claimsVec);
        for (int i = 0; i < rowCount; i++) {
            writer.writeEncoded(i, keys, values);
        }
        // The batch writer's setRowCount stamped this vector's value count while it was
        // still empty — refresh it now that the maps are written.
        claimsVec.setValueCount(rowCount);
    }
}
