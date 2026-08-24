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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.helpers.Numbers;
import org.neo4j.internal.recordstorage.Command.MetaDataCommand;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.wal.entry.LogFormat;

public class KernelVersionTransactionApplier extends TransactionApplier.Adapter {
    private final MetadataCache metadataCache;
    private final InternalLog log;

    public KernelVersionTransactionApplier(MetadataCache metadataCache, InternalLogProvider userLogProvider) {
        this.metadataCache = metadataCache;
        this.log = userLogProvider.getLog(getClass());
    }

    @Override
    public boolean visitMetaDataCommand(MetaDataCommand command) {
        int value = Numbers.safeCastLongToInt(command.getAfter().getValue());
        final var kernelVersion = KernelVersion.getForVersion((byte) (value & 0xFF));
        // Not using the format yet, that is coming soon
        byte logFormatVersion = (byte) ((value >> Byte.SIZE) & 0xFF);
        log.info("Applying metadata upgrade command: " + command + " previous KernelVersion "
                + metadataCache.kernelVersion() + " previous LogFormat " + metadataCache.getCurrentLogFormat());
        metadataCache.setKernelVersion(kernelVersion);
        metadataCache.setCurrentLogFormat(
                logFormatVersion == 0
                        ? LogFormat.fromKernelVersion(kernelVersion)
                        : LogFormat.fromByteVersion(logFormatVersion));
        return false;
    }
}
