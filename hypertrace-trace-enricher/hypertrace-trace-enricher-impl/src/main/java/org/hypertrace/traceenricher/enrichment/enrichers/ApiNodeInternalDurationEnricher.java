package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.SpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiNodeInternalDurationEnricher extends AbstractTraceEnricher {

  private static final Logger LOG = LoggerFactory.getLogger(ApiNodeInternalDurationEnricher.class);

  @Override
  public void enrichTrace(StructuredTrace trace) {

    try {
      ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
      List<ApiNode<Event>> apiNodes = apiTraceGraph.getApiNodeList();

      for (ApiNode<Event> apiNode : apiNodes) {
        Optional<Event> entryEventMaybe = apiNode.getEntryApiBoundaryEvent();
        entryEventMaybe.ifPresent(
            entryEvent -> {
              // todo: Consider only those EXIT events that are CHILD_OF
              // we normalise all EXIT calls and Outbound edges to NormalizedOutboundEdge
              List<NormalizedOutboundEdge> normalizedOutboundEdges =
                  getNormalizedOutboundEdges(apiTraceGraph, apiNode);
              // then sort these edges in ascending order of start times
              normalizedOutboundEdges.sort(
                  (o1, o2) -> (int) (o1.startTimeMillis - o2.startTimeMillis));
              var entryApiBoundaryEventDuration =
                  SpanUtils.getMetricValue(entryEvent, "Duration", -1);
              var totalWaitTime = 0L;
              if (normalizedOutboundEdges.size() > 0) {
                totalWaitTime = calculateTotalWaitTime(normalizedOutboundEdges);
              }
              enrichSpan(entryEvent, entryApiBoundaryEventDuration, totalWaitTime);
            });
      }
    } catch (Exception e) {
      LOG.error(
          "Exception enriching trace: {} for internal duration",
          HexUtils.getHex(trace.getTraceId()));
    }
  }

  private List<NormalizedOutboundEdge> getNormalizedOutboundEdges(
      ApiTraceGraph apiTraceGraph, ApiNode<Event> apiNode) {
    List<NormalizedOutboundEdge> normalizedOutboundEdges =
        apiNode.getExitApiBoundaryEvents().stream()
            .map(
                event ->
                    NormalizedOutboundEdge.from(
                        event.getStartTimeMillis(), event.getEndTimeMillis()))
            .collect(Collectors.toList());
    //    apiTraceGraph.getOutboundEdgesForApiNode(apiNode).stream()
    //        .map(
    //            edge -> NormalizedOutboundEdge.from(edge.getStartTimeMillis(),
    // edge.getEndTimeMillis()))
    //        .forEach(normalizedOutboundEdges::add);
    return normalizedOutboundEdges;
  }

  /*
  The algo look at two edges at a time and check if they're sequential or parallel. For sequential edges, wait time is simply
  the sum of the two. However, for parallel edges (in a group), the algorithm simply takes the longest span in the group and adds it to the
  total wait time.
  Examples:
  //   [--------d1---------]
  //                        [--------d2----------]
  //                                             [-------d3----]
  // Total wait time = d1 + d2 + d3. All spans are sequential.

  // [----d1-----]
  //                t1 [-------------------] t2
  //                    t3 [----------] t4
  //                          t5 [----------------] t6
  //                                t7 [--------------------------] t8
  // In this case, the total wait time is d1 + max((t2 - t1), (t4 - t3), (t6 - t5), (t8 - t7)) = d1 + (t8 - t7)
  // Parallel requests are typically submitted to an executor service which then reads requests from a work queue and assigns them to workers based
  // on their availability. However, we must exclude any time the application spends on thread availability. We assume the best case: The ES has unlimited
  // no of threads and it fires requests as soon as they're submitted. So the above diagram becomes:
  // [----d1-----]
  //                t1 [-------------------] t2
  //                t3 [----------] t4
  //                t5 [----------------] t6
  //                t7 [--------------------------] t8
  // So virtually, all requests start from almost the same point. The application waits till it gets the final response, which is represented by the longest span in the group.
   */
  @VisibleForTesting
  long calculateTotalWaitTime(List<NormalizedOutboundEdge> outboundEdges) {
    long totalWait = 0;
    long maxRunningEndTime = outboundEdges.get(0).getEndTimeMillis();
    long runningWaitTime = outboundEdges.get(0).getDuration();
    for (int i = 0; i < outboundEdges.size() - 1; i++) {
      var lookaheadEdge = outboundEdges.get(i + 1);
      if (isSequential(maxRunningEndTime, lookaheadEdge)) {
        // .....-----] maxRunningEndTime
        //             [----lookahead edge---]
        // if lookahead edge is sequential in the series, we add the running wait time to the total
        // wait.
        totalWait += runningWaitTime;
        // since it's sequential, maxRunningEndTime is simply the end time of the lookahead edge
        maxRunningEndTime = lookaheadEdge.getEndTimeMillis();
        // the new running wait time becomes the duration of this edge.
        runningWaitTime = lookaheadEdge.getDuration();
      } else {
        // if lookahead edge is parallel in the series, then maxRunningEndTime can either the end
        // time of the lookahead edge as below:
        // [------------]
        //       [---LA EDGE----]
        // or it can be the running maxRunningEndTime itself:
        // [-------------]
        //   [-------]
        maxRunningEndTime = Math.max(maxRunningEndTime, lookaheadEdge.getEndTimeMillis());
        // the runningWaitTime is simply the longest span in the group of parallel spans.
        runningWaitTime = Math.max(runningWaitTime, lookaheadEdge.getDuration());
      }
    }
    // to compensate for the remaining last iteration
    totalWait += runningWaitTime;
    return totalWait;
  }

  private boolean isSequential(long endTimeMillis, NormalizedOutboundEdge lookaheadEdge) {
    return lookaheadEdge.getStartTimeMillis() >= endTimeMillis;
  }

  private void enrichSpan(
      Event entryEvent, double entryApiBoundaryEventDuration, long totalWaitTime) {
    // enriched attributes
    entryEvent
        .getAttributes()
        .getAttributeMap()
        .put(
            EnrichedSpanConstants.API_INTERNAL_DURATION,
            AttributeValueCreator.create(
                String.valueOf(entryApiBoundaryEventDuration - totalWaitTime)));
    // metric map
    entryEvent
        .getMetrics()
        .getMetricMap()
        .put(
            EnrichedSpanConstants.API_INTERNAL_DURATION,
            fastNewBuilder(MetricValue.Builder.class)
                .setValue(entryApiBoundaryEventDuration - totalWaitTime)
                .build());
  }

  static class NormalizedOutboundEdge {

    private final long startTimeMillis;
    private final long endTimeMillis;
    private final long duration;

    NormalizedOutboundEdge(long startTimeMillis, long endTimeMillis) {
      this.startTimeMillis = startTimeMillis;
      this.endTimeMillis = endTimeMillis;
      this.duration = endTimeMillis - startTimeMillis;
    }

    public static NormalizedOutboundEdge from(long startTimeMillis, long endTimeMillis) {
      return new NormalizedOutboundEdge(startTimeMillis, endTimeMillis);
    }

    public long getStartTimeMillis() {
      return startTimeMillis;
    }

    public long getEndTimeMillis() {
      return endTimeMillis;
    }

    public long getDuration() {
      return duration;
    }
  }
}
