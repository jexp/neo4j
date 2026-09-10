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
package org.neo4j.kernel.api.impl.schema.vector;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.v10.codec.RawVectorsReadAdvice;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;

/// The vector index provider's directory factory, deciding per opened directory whether the raw
/// vector read advice applies, see [LuceneSettings#vector_rescore_read_advice].
///
final class RescoreReadAdviceDirectoryFactory implements DirectoryFactory {
    private final DirectoryFactory delegate;
    private final boolean rescoreReadAdvice;

    RescoreReadAdviceDirectoryFactory(DirectoryFactory delegate, Config config) {
        this.delegate = delegate;
        this.rescoreReadAdvice = config.get(LuceneSettings.vector_rescore_read_advice);
    }

    @Override
    public LuceneDirectory open(Path dir) throws IOException {
        LuceneDirectory directory = delegate.open(dir);
        if (rescoreReadAdvice) {
            RawVectorsReadAdvice.adviseFor(directory);
        }
        return directory;
    }

    @Override
    public LuceneContext getContext() {
        return delegate.getContext();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
