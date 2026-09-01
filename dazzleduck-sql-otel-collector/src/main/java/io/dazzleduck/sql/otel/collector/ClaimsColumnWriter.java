package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.util.Text;

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
        UnionMapWriter writer = claimsVec.getWriter();

        // Single entrySet pass: encodes once per batch and keeps key/value pairing guaranteed.
        Text[] keys = new Text[claims.size()];
        Text[] values = new Text[claims.size()];
        int n = 0;
        for (Map.Entry<String, String> claim : claims.entrySet()) {
            keys[n] = new Text(claim.getKey());
            values[n] = new Text(claim.getValue());
            n++;
        }

        for (int i = 0; i < rowCount; i++) {
            writer.setPosition(i);
            writer.startMap();
            for (int k = 0; k < keys.length; k++) {
                OtelSchemaFields.writeEntry(writer, keys[k], values[k]);
            }
            writer.endMap();
        }
        // The batch writer's setRowCount stamped this vector's value count while it was
        // still empty — refresh it now that the maps are written.
        claimsVec.setValueCount(rowCount);
    }
}
