/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.lucene;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.values.storable.Value;

public interface LuceneIndexSearcher extends Closeable {
    static int getMaxClauseCount() {
        return IndexSearcher.getMaxClauseCount();
    }

    LuceneDocument doc(int docId) throws IOException;

    IndexProgressor searchDocValues(LuceneQueryContext queryContext, String field, EntityConsumer entityConsumer)
            throws IOException;

    IndexProgressor searchDocValues(
            LuceneQueryContext queryContext, String field, IndexProgressor.EntityValueClient client) throws IOException;

    ValuesIterator searchVectors(LuceneQueryContext queryContext, IndexQueryConstraints constraints) throws IOException;

    List<LuceneDocument> searchTopN(LuceneQueryContext queryContext, int n) throws IOException;

    int count(LuceneQueryContext queryContext) throws IOException;

    LuceneQueryContext rewrite(LuceneQueryContext queryContext) throws IOException;

    LuceneQueryContext newQueryContext();

    /** The Lucene leaf contexts of this partition's current snapshot. */
    default List<LeafReaderContext> leafContexts() {
        return List.of();
    }

    LucenePartitionedSearch newPartitionedSearcher(int size);

    LuceneAllDocumentsReader newAllDocumentsReader();

    /**
     * Returns the number of documents in this index.
     * @return number of documents in this index.
     */
    int numDocs();

    IndexSampler newIndexSampler(TaskCoordinator taskCoordinator, IndexSamplingConfig samplingConfig);

    @FunctionalInterface
    interface EntityConsumer {
        boolean acceptEntity(long reference, float score, Value... values);
    }

    final class InRangeEntityConsumer implements EntityConsumer {
        private final long fromIdInclusive;
        private final long toIdExclusive;

        private long reference;

        public InRangeEntityConsumer(long fromIdInclusive, long toIdExclusive) {
            this.fromIdInclusive = fromIdInclusive;
            this.toIdExclusive = toIdExclusive;
        }

        public long reference() {
            return reference;
        }

        @Override
        public boolean acceptEntity(long reference, float score, Value... values) {
            if (fromIdInclusive <= reference && reference < toIdExclusive) {
                this.reference = reference;
                return true;
            }

            return false;
        }
    }
}
