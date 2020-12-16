package org.greenplum.pxf.api.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.catalina.connector.ClientAbortException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 */
@EqualsAndHashCode(of = {"queryId"})
public class QuerySession {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySession.class);

    /**
     * A unique identifier for the query
     */
    @Getter
    private final String queryId;

    /**
     * The request context for the given query
     */
    @Getter
    private final RequestContext context;

    /**
     * The processor used for this query session
     */
    @Getter
    private final Processor<?> processor;

    /**
     * True if the producers have finished producing, false otherwise
     */
    private final AtomicBoolean finishedProducing;

    /**
     * True if the query has been cancelled, false otherwise
     */
    private final AtomicBoolean queryCancelled;

    /**
     * True if the query has errors, false otherwise
     */
    private final AtomicBoolean queryErrored;

    /**
     * Records the Instant when the query was created
     */
    private final Instant startTime;

    /**
     * Records the Instant when the query was cancelled, null if the query was
     * not cancelled
     */
    private Instant cancelTime;

    /**
     * Records the Instant when the first error occurred in the query, null
     * if there are no errors
     */
    private Instant errorTime;

    /**
     * A queue of the errors encountered during processing of this query
     * session.
     */
    @Getter
    private final Deque<Exception> errors;

    /**
     * The list of splits for the query. The list of splits is only set when
     * the "Metadata" cache is enabled
     */
    @Getter
    @Setter
    private volatile List<DataSplit> dataSplitList;

    /**
     * A queue of segments that have registered to this query session
     */
    @Getter
    private final BlockingDeque<Integer> registeredSegmentQueue;

    /**
     * The queue used to process tuples.
     */
    @Getter
    private final BlockingDeque<List<List<Object>>> outputQueue;

    /**
     * Number of active segments that have registered to this QuerySession
     */
    private final AtomicInteger activeSegmentCount;

    /**
     * Tracks number of active tasks
     */
    private final AtomicInteger createdTaskCount;

    /**
     * Tracks number of completed tasks
     */
    private final AtomicInteger completedTaskCount;

    /**
     * The total number of tuples that were streamed out to the client
     */
    private final AtomicLong totalTupleCount;

    public QuerySession(RequestContext context, Processor<?> processor) {
        this.context = context;
        this.processor = processor;
        this.queryId = String.format("%s:%s:%s:%s", context.getServerName(),
                context.getTransactionId(), context.getDataSource(), context.getFilterString());
        this.startTime = Instant.now();
        this.finishedProducing = new AtomicBoolean(false);
        this.queryCancelled = new AtomicBoolean(false);
        this.queryErrored = new AtomicBoolean(false);
        this.registeredSegmentQueue = new LinkedBlockingDeque<>();
        this.outputQueue = new LinkedBlockingDeque<>(200);
        this.errors = new ConcurrentLinkedDeque<>();
        this.activeSegmentCount = new AtomicInteger(0);
        this.createdTaskCount = new AtomicInteger(0);
        this.completedTaskCount = new AtomicInteger(0);
        this.totalTupleCount = new AtomicLong(0);
    }

    /**
     * Cancels the query, the first thread to cancel the query sets the cancel
     * time
     */
    public void cancelQuery(ClientAbortException e) {
        if (!queryCancelled.getAndSet(true)) {
            cancelTime = Instant.now();
        }
        errors.offer(e);
    }

    /**
     * Registers a segment to this query session
     *
     * @param segmentId the segment identifier
     */
    public void registerSegment(int segmentId) throws InterruptedException {
        if (!isActive()) {
            LOG.debug("Skip registering processor because the query session is no longer active");
            return;
        }

        activeSegmentCount.incrementAndGet();
        registeredSegmentQueue.put(segmentId);
    }

    /**
     * De-registers a segment, and the last segment to deregister the endTime
     * is recorded.
     *
     * @param recordCount the recordCount from the segment
     */
    public void deregisterSegment(long recordCount) {

        totalTupleCount.addAndGet(recordCount);

        if (activeSegmentCount.decrementAndGet() == 0) {
            Instant endTime = Instant.now();
            outputQueue.clear(); // unblock producers in the case of error
            dataSplitList = null; // release references

            long totalRecords = totalTupleCount.get();

            if (errorTime != null) {

                long durationMs = Duration.between(startTime, errorTime).toMillis();
                LOG.info("{} errored after {}ms", this, durationMs);

            } else if (cancelTime != null) {

                long durationMs = Duration.between(startTime, cancelTime).toMillis();
                LOG.info("{} canceled after {}ms", this, durationMs);

            } else {

                long durationMs = Duration.between(startTime, endTime).toMillis();
                double rate = durationMs == 0 ? 0 : (1000.0 * totalRecords / durationMs);

                LOG.info("{} completed streaming {} tuple{} in {}ms. {} tuples/sec",
                        this,
                        totalRecords,
                        totalRecords == 1 ? "" : "s",
                        durationMs,
                        String.format("%.2f", rate));

            }
        }
    }

    /**
     * Marks the query as errored, the first thread to error the query sets
     * the error time
     */
    public void errorQuery(Exception e) {
        if (!queryErrored.getAndSet(true)) {
            errorTime = Instant.now();
        }
        errors.offer(e);
    }

    /**
     * Returns the number of tasks that have completed
     *
     * @return the number of tasks that have completed
     */
    public int getCompletedTaskCount() {
        return completedTaskCount.get();
    }

    /**
     * Returns the number of tasks that have been created
     *
     * @return the number of tasks that have been created
     */
    public int getCreatedTaskCount() {
        return createdTaskCount.get();
    }

    /**
     * Whether this query session has finished producing tuples or not
     *
     * @return true if this query session has finished producing tuples, false otherwise
     */
    public boolean hasFinishedProducing() {
        return finishedProducing.get();
    }

    /**
     * Determines whether the query session is active. The query session
     * becomes inactive if the query is errored or the query is cancelled.
     *
     * @return true if the query is active, false when the query has errors or is cancelled
     */
    public boolean isActive() {
        return !isQueryErrored() && !isQueryCancelled();
    }

    /**
     * Check whether the query has errors
     *
     * @return true if the query has errors, false otherwise
     */
    public boolean isQueryErrored() {
        return queryErrored.get();
    }

    /**
     * Check whether the query was cancelled
     *
     * @return true if the query was cancelled, false otherwise
     */
    public boolean isQueryCancelled() {
        return queryCancelled.get();
    }

    /**
     * Signal that this query session has finished producing splits
     */
    public void markAsFinishedProducing() {
        finishedProducing.set(true);
    }

    /**
     * Keeps track of tasks that have been created
     */
    public void registerTask() {
        createdTaskCount.incrementAndGet();
    }

    /**
     * Keeps track of tasks that have completed
     */
    public void registerCompletedTask() {
        completedTaskCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "QuerySession@" +
                Integer.toHexString(System.identityHashCode(this)) +
                "{queryId='" + queryId + '\'' +
                '}';
    }
}