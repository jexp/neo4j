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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.kernel.api.impl.index.lucene.Abortable;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.lucene.v9.codec.Lucene9Codec;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.shaded.lucene9.codecs.Codec;
import org.neo4j.shaded.lucene9.codecs.CodecUtil;
import org.neo4j.shaded.lucene9.index.CheckIndex;
import org.neo4j.shaded.lucene9.index.ConcurrentMergeScheduler;
import org.neo4j.shaded.lucene9.index.DirectoryReader;
import org.neo4j.shaded.lucene9.index.IndexWriter;
import org.neo4j.shaded.lucene9.index.IndexWriterConfig;
import org.neo4j.shaded.lucene9.index.KeepOnlyLastCommitDeletionPolicy;
import org.neo4j.shaded.lucene9.index.LogByteSizeMergePolicy;
import org.neo4j.shaded.lucene9.index.MergePolicy;
import org.neo4j.shaded.lucene9.index.MergePolicy.OneMerge;
import org.neo4j.shaded.lucene9.index.MergeScheduler;
import org.neo4j.shaded.lucene9.index.MergeScheduler.MergeSource;
import org.neo4j.shaded.lucene9.index.MergeTrigger;
import org.neo4j.shaded.lucene9.index.SegmentInfos;
import org.neo4j.shaded.lucene9.index.SegmentInfos.FindSegmentsFile;
import org.neo4j.shaded.lucene9.index.SerialMergeScheduler;
import org.neo4j.shaded.lucene9.index.SnapshotDeletionPolicy;
import org.neo4j.shaded.lucene9.index.TieredMergePolicy;
import org.neo4j.shaded.lucene9.store.Directory;
import org.neo4j.shaded.lucene9.store.IOContext;
import org.neo4j.shaded.lucene9.store.IndexInput;
import org.neo4j.shaded.lucene9.util.Version;

public class Lucene9Directory implements LuceneDirectory {
    final Directory directory;

    public Lucene9Directory(Directory directory) {
        this.directory = directory;
    }

    @Override
    public String[] listAll() throws IOException {
        return directory.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        directory.deleteFile(name);
    }

    @Override
    public boolean indexExists() throws IOException {
        return DirectoryReader.indexExists(directory);
    }

    @Override
    public LuceneDirectoryReader open() throws IOException {
        return new Lucene9DirectoryReader(DirectoryReader.open(directory));
    }

    @Override
    public boolean checkIndexIsClean() throws IOException {
        try (CheckIndex checkIndex = new CheckIndex(directory)) {
            CheckIndex.Status status = checkIndex.checkIndex();
            return status.clean;
        }
    }

    @Override
    public boolean validVersion() throws IOException {
        int createdMajorVersion = new CreatedMajorVersion().run();
        return Version.MIN_SUPPORTED_MAJOR <= createdMajorVersion && createdMajorVersion <= Version.LATEST.major;
    }

    @Override
    public boolean hasCommits() throws IOException {
        return indexExists() && SegmentInfos.readLatestCommit(directory) != null;
    }

    @Override
    public Collection<String> latestCommitFileNames() throws IOException {
        return DirectoryReader.listCommits(directory).getLast().getFileNames();
    }

    @Override
    public LuceneIndexWriter newWriter(LuceneIndexWriterConfig writerConfig) throws IOException {
        IndexWriter indexWriter = new IndexWriter(directory, convertConfig(writerConfig));
        return new Lucene9IndexWriter(indexWriter);
    }

    @Override
    public LuceneContext getLuceneContext() {
        return LuceneContext.LUCENE_9;
    }

    @Override
    public void close() throws IOException {
        directory.close();
    }

    byte[] readFile(String name) throws IOException {
        try (IndexInput in = directory.openInput(name, IOContext.DEFAULT)) {
            byte[] bytes = new byte[toIntExact(in.length())];
            in.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    private final class CreatedMajorVersion extends FindSegmentsFile<Integer> {
        private CreatedMajorVersion() {
            super(directory);
        }

        @Override
        protected Integer doBody(String segmentFileName) throws IOException {
            try (IndexInput input = directory.openInput(segmentFileName, IOContext.DEFAULT)) {
                CodecUtil.readIndexHeader(input);
                input.readVInt(); // Version.major
                input.readVInt(); // Version.minor
                input.readVInt(); // Version.bugfix
                return input.readVInt();
            }
        }
    }

    private static IndexWriterConfig convertConfig(LuceneIndexWriterConfig config) {
        if (config.analyzerOnly) {
            return new IndexWriterConfig(Lucene9Utils.loadAnalyzer(config.analyzer));
        }

        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Lucene9Utils.loadAnalyzer(config.analyzer));
        if (config.RAMBufferSizeMB != null) {
            indexWriterConfig.setRAMBufferSizeMB(config.RAMBufferSizeMB);
        }
        if (config.maxBufferedDocs != null) {
            indexWriterConfig.setMaxBufferedDocs(config.maxBufferedDocs);
        }

        indexWriterConfig.setCommitOnClose(true);
        indexWriterConfig.setUseCompoundFile(true);
        indexWriterConfig.setMaxFullFlushMergeWaitMillis(0);
        indexWriterConfig.setIndexDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()));

        if (config.codec != null) {
            indexWriterConfig.setCodec(toInternalCodec(config.codec));
        }

        MergeScheduler mergeScheduler;
        if (config.useOnThreadConcurrentMergeScheduler) {
            mergeScheduler = new OnThreadConcurrentMergeScheduler();
        } else {
            ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
            cms.setDefaultMaxMergesAndThreads(false);
            mergeScheduler = cms;
        }
        indexWriterConfig.setMergeScheduler(new LoggedMergeScheduler(mergeScheduler, config.logProvider));
        indexWriterConfig.setMergePolicy(mergePolicy(config));

        return indexWriterConfig;
    }

    private static MergePolicy mergePolicy(LuceneIndexWriterConfig config) {
        return switch (config.mergePolicyOption) {
            case LOG_BYTE_SIZED -> {
                LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
                mergePolicy.setNoCFSRatio(config.noCFSRatio);
                mergePolicy.setMaxCFSSegmentSizeMB(config.maxCFSSegmentSizeMB);
                mergePolicy.setMinMergeMB(config.minMergeMB);
                mergePolicy.setMaxMergeMB(config.maxMergeMB);
                mergePolicy.setMergeFactor(config.mergeFactor);
                yield mergePolicy;
            }
            case TIERED -> {
                TieredMergePolicy mergePolicy = new TieredMergePolicy();
                mergePolicy.setNoCFSRatio(config.noCFSRatio);
                mergePolicy.setMaxCFSSegmentSizeMB(config.maxCFSSegmentSizeMB);
                mergePolicy.setSegmentsPerTier(config.segmentsPerTier);
                mergePolicy.setMaxMergeAtOnce(config.maxMergeAtOnce);
                yield mergePolicy;
            }
        };
    }

    private static Codec toInternalCodec(LuceneCodec codec) {
        return ((Lucene9Codec) codec).codec();
    }

    /**
     * This is a {@link MergeScheduler} which is a version of {@link SerialMergeScheduler},
     * but with the important difference that multiple threads can run merge of difference sources in parallel.
     * I.e. in the scenario of index population where the population threads that adds documents go and do merge
     * on their individual threads, in parallel with the other population threads. This effectively comes close
     * to the {@link ConcurrentMergeScheduler} parallel-wise w/o spawning additional
     * background threads.
     */
    static class OnThreadConcurrentMergeScheduler extends MergeScheduler implements Abortable {
        private volatile boolean aborted;
        private final Set<MergePolicy.OneMerge> merging = ConcurrentHashMap.newKeySet();

        @Override
        public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
            MergePolicy.OneMerge merge;
            while ((merge = nextMergeSynchronized(mergeSource)) != null) {
                if (aborted) {
                    // Abandon it rather than run it. Reporting it as finished matters: forceMerge() waits for every
                    // merge it queued, so simply declining to take them would leave it waiting forever.
                    merge.setAborted();
                    mergeSource.onMergeFinished(merge);
                    continue;
                }
                merging.add(merge);
                try {
                    mergeSource.merge(merge);
                } finally {
                    merging.remove(merge);
                }
            }
        }

        @Override
        public void abort() {
            aborted = true;
            merging.forEach(MergePolicy.OneMerge::setAborted);
        }

        private synchronized MergePolicy.OneMerge nextMergeSynchronized(MergeSource mergeSource) {
            return mergeSource.getNextMerge();
        }

        @Override
        public void close() {}
    }

    /**
     * This is a delegate wrapper around a {@link MergeScheduler} to inject some custom logging
     */
    private static final class LoggedMergeScheduler extends MergeScheduler implements Abortable {
        private final MergeScheduler delegate;
        private final Log log;

        private LoggedMergeScheduler(MergeScheduler delegate, LogProvider logProvider) {
            this.delegate = delegate;
            this.log = logProvider.getLog(getClass());
        }

        @Override
        public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
            LogMergeListener mergeListener = new LogMergeListener(log, trigger.name());
            try (mergeListener) {
                mergeSource = new LoggedMergeSource(mergeSource, mergeListener);
                delegate.merge(mergeSource, trigger);
            } catch (Exception e) {
                mergeListener.thrown(e);
                throw e;
            }
        }

        @Override
        public void abort() {
            if (delegate instanceof Abortable abortable) {
                abortable.abort();
            }
        }
    }

    /**
     * This is a delegate wrapper around a {@link MergeSource} to inject some custom logging
     */
    private record LoggedMergeSource(MergeSource delegate, LogMergeListener mergeListener) implements MergeSource {
        @Override
        public OneMerge getNextMerge() {
            return delegate.getNextMerge();
        }

        @Override
        public void onMergeFinished(OneMerge merge) {
            delegate.onMergeFinished(merge);
        }

        @Override
        public boolean hasPendingMerges() {
            return delegate.hasPendingMerges();
        }

        @Override
        public void merge(OneMerge merge) throws IOException {
            try {
                delegate.merge(merge);
                mergeListener.individualMergeInfo(
                        merge.totalNumDocs(), merge.totalBytesSize(), merge.estimatedMergeBytes);
            } catch (Exception e) {
                mergeListener.thrown(e);
                throw e;
            }
        }
    }
}
