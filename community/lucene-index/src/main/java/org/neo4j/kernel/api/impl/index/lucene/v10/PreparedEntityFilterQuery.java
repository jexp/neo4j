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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.neo4j.kernel.api.impl.index.lucene.LucenePreparedEntityFilter;

/**
 * Filter query backed by an already-resolved per-segment bitset. Unlike {@link EntityIdSetQuery}
 * this performs no doc-values scan and no automaton intersection — the docIds are already known.
 */
final class PreparedEntityFilterQuery extends Query {
    private final LucenePreparedEntityFilter prepared;

    static Query create(LucenePreparedEntityFilter prepared) {
        return new PreparedEntityFilterQuery(prepared);
    }

    private PreparedEntityFilterQuery(LucenePreparedEntityFilter prepared) {
        this.prepared = prepared;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                BitSet bits =
                        prepared.bitSetFor(context.reader().getCoreCacheHelper().getKey());
                if (bits == null) {
                    return null;
                }
                int cardinality = bits.cardinality();
                if (cardinality == 0) {
                    return null;
                }
                var iterator = new BitSetIterator(bits, cardinality);
                return new DefaultScorerSupplier(new ConstantScoreScorer(1f, scoreMode, iterator));
            }
        };
    }

    @Override
    public String toString(String field) {
        return field + ":<prepared-entity-filter>";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    // NOTE: referential equality — the prepared filter is snapshot-bound and never cached.
    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
