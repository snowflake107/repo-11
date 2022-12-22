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

  private List<NormalizedOutboundEdge> getNormalizedOutboundEdges(
      ApiTraceGraph apiTraceGraph, ApiNode<Event> apiNode) {
    List<NormalizedOutboundEdge> normalizedOutboundEdges =
        apiNode.getExitApiBoundaryEvents().stream()
            .map(
                event ->
                    NormalizedOutboundEdge.from(
                        event.getStartTimeMillis(), event.getEndTimeMillis()))
            .collect(Collectors.toList());
    apiTraceGraph.getOutboundEdgesForApiNode(apiNode).stream()
        .map(
            edge -> NormalizedOutboundEdge.from(edge.getStartTimeMillis(), edge.getEndTimeMillis()))
        .forEach(normalizedOutboundEdges::add);
    return normalizedOutboundEdges;
  }

  /*
  The algo look at two edges at a time and check if they're sequential or parallel. For sequential edges, wait time is simply
  the sum of the two. However, for parallel edges (in a group), the algorithm creates a virtual edge from the start of the very first
  edge in the group to the end of the very last edge in the group. This is the approximate total wait time for the group:
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
  //                  <----------------virtual edge--------------->
  // In this case, the total wait time is d1 + duration(virtual edge) = d1 + (t8 - t1)
  //
  // The virtual edge is a reasonable approximation to the total wait time because the application starts waiting as soon as all the requests are submitted asynchronously
  // which is very fast, and can be approximated by the start time of the very first span. The wait ends once the very last response is received. This obviously is not very correct because
  // there might be thread scheduling delays for n no. of reasons. What if there is a substantial time difference b/w when the application submitted all the requests and
  // the time when the very first request is executed? Such scheduling delays can either be the thread is busy executing a previous network request (in which we should add the scheduling delay to the WAIT time)
  // or it is busy doing something else internally (in which we should not add the scheduling delay to the wait time). It is impossible to divide such waits b/w application time and network time using just trace data.
   */
  @VisibleForTesting
  long calculateTotalWaitTime(List<NormalizedOutboundEdge> outboundEdges) {
    long totalWait = 0;
    long startTime = outboundEdges.get(0).getStartTimeMillis();
    long endTime = outboundEdges.get(0).getEndTimeMillis();
    for (int i = 0; i < outboundEdges.size() - 1; i++) {
      // we call it a virtual current edge because for parallel spans, we create an edge that spans
      // multiple spans
      var virtualCurrEdge = NormalizedOutboundEdge.from(startTime, endTime);
      var lookaheadEdge = outboundEdges.get(i + 1);
      if (areSequential(virtualCurrEdge, lookaheadEdge)) {
        // if curr edge and lookahead edge are sequential, we simply update the total duration
        totalWait += virtualCurrEdge.getDuration();
        // the new virtual edge becomes the lookahead edge. This virtual edge is also an actual
        // edge.
        startTime = lookaheadEdge.getStartTimeMillis();
        endTime = lookaheadEdge.getEndTimeMillis();
      } else {
        // if curr edge and lookahead edge are parallel, then we simply update the next virtual
        // edge. This is the running wait time
        // till we find a node sequential to the running virtual edge in subsequent iterations.
        // Cases:
        // [-----------------]
        //      [---------------]
        // <----------VE-------->

        // [-----------------]
        //     [-------]
        // <--------VE------->

        // [------------------]
        //                 [----]
        // <--------VE---------->
        startTime =
            Math.min(virtualCurrEdge.getStartTimeMillis(), lookaheadEdge.getStartTimeMillis());
        endTime = Math.max(virtualCurrEdge.getEndTimeMillis(), lookaheadEdge.getEndTimeMillis());
      }
    }
    totalWait += (endTime - startTime);
    return totalWait;
  }

  private boolean areSequential(
      NormalizedOutboundEdge currEdge, NormalizedOutboundEdge lookaheadEdge) {
    return lookaheadEdge.getStartTimeMillis() >= currEdge.getEndTimeMillis();
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
