package org.greenplum.pxf.api.task;

import com.google.common.collect.Lists;
import org.greenplum.pxf.api.concurrent.BoundedExecutor;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitSegmentIterator;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.greenplum.pxf.api.configuration.PxfServerProperties.DEFAULT_SCALE_FACTOR;
import static org.greenplum.pxf.api.factory.ConfigurationFactory.PXF_PROCESSOR_SCALE_FACTOR_PROPERTY;

public class ProducerTask<T> implements Runnable {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final QuerySession<T> querySession;
    private final BoundedExecutor boundedExecutor;

    public ProducerTask(QuerySession<T> querySession, Executor executor) {
        this.querySession = requireNonNull(querySession, "querySession cannot be null");
        // maxProcessorThreads defaults to MAX_PROCESSOR_THREADS_PER_SESSION
        // it can be overridden by setting the PXF_MAX_PROCESSOR_THREADS_PROPERTY
        // in the server configuration
        int scaleFactor = querySession.getContext().getConfiguration()
                .getInt(PXF_PROCESSOR_SCALE_FACTOR_PROPERTY, DEFAULT_SCALE_FACTOR);
        int maxProcessorThreads = Utilities.getProcessorMaxThreadsPerSession(scaleFactor);
        this.boundedExecutor = new BoundedExecutor(executor, maxProcessorThreads);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        int totalSegments = querySession.getContext().getTotalSegments();
        Integer segmentId;
        Set<Integer> segmentIds;

        try {
            // Materialize the list of splits
            DataSplitter splitter = querySession.getProcessor().getDataSplitter(querySession);
            List<DataSplit> splitList = Lists.newArrayList(splitter);
            // get the queue of segments IDs that have registered to this QuerySession
            BlockingDeque<Integer> registeredSegmentQueue = querySession.getRegisteredSegmentQueue();

            while (querySession.isActive()) {
                segmentId = registeredSegmentQueue.poll(20, TimeUnit.MILLISECONDS);

                if (segmentId == null) {
                    int completed = querySession.getCompletedTupleReaderTaskCount();
                    int created = querySession.getCreatedTupleReaderTaskCount();
                    if (completed == created) {
                        // try to mark the session as inactive. If another
                        // thread is able to register itself before we mark it
                        // as inactive, or the output queue has elements
                        // still remaining to process, this operation will be
                        // a no-op
                        querySession.tryMarkInactive();
                    }
                } else {
                    // Add all the segment ids that are available
                    segmentIds = new HashSet<>();
                    segmentIds.add(segmentId);
                    while ((segmentId = registeredSegmentQueue.poll(5, TimeUnit.MILLISECONDS)) != null) {
                        segmentIds.add(segmentId);
                    }

                    LOG.debug("ProducerTask with a set of {} segmentId(s)", segmentIds.size());

                    Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(segmentIds, totalSegments, splitList);
                    while (iterator.hasNext() && querySession.isActive()) {
                        DataSplit split = iterator.next();
                        LOG.debug("Submitting {} to the pool for query {}", split, querySession);

                        TupleReaderTask<T> task = new TupleReaderTask<>(split, querySession);

                        querySession.markTaskAsCreated();
                        boundedExecutor.execute(task);
                    }
                }
            }
        } catch (Exception ex) {
            querySession.errorQuery(ex);
            throw new RuntimeException(ex);
        } finally {

            // At this point the query session is inactive for one of the
            // following reasons:
            //   1. There is no more work or tuples available to be processed
            //   2. The query was cancelled
            //   3. The query errored out

            if (querySession.isQueryErrored() || querySession.isQueryCancelled()) {
                // When an error occurs or the query is cancelled, we need
                // to cancel all the running TupleReaderTasks.
                boundedExecutor.shutdownNow();
                cancelAllTasks();
                tasks.clear();
                boundedExecutor.awaitTermination(1000, TimeUnit.SECONDS);
                // TODO drain outputQueue
            }

            try {
                // We need to allow all of the ScanResponses to fully consume
                // the batches from the outputQueue
                // Wait with timeout in case we miss the signal. If we
                // miss the signal we don't want to wait forever.
                querySession.waitForAllSegmentsToDeregister();
            } catch (InterruptedException ignored) {
            }

            try {
                querySession.close();
            } catch (Exception ex) {
                LOG.warn(String.format("Error while closing the QuerySession %s", querySession), ex);
            }

            // Clean the interrupted flag
            Thread.interrupted();
        }
    }

    private void cancelAllTasks() {
        for (TupleReaderTask<T> task : tasks) {
            task.cancel();
        }
    }
}
