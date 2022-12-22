package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;

public class ApiNodeInternalDurationEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichTrace(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    List<ApiNode<Event>> apiNodes = apiTraceGraph.getApiNodeList();

    for (ApiNode<Event> apiNode : apiNodes) {
      Optional<Event> entryEvent = apiNode.getEntryApiBoundaryEvent();
      List<OutboundEdge> outboundEdges =
          apiNode.getExitApiBoundaryEvents().stream()
              .map(event -> OutboundEdge.from(event.getStartTimeMillis(), event.getEndTimeMillis()))
              .collect(Collectors.toList());
      apiTraceGraph.getOutboundEdgesForApiNode(apiNode).stream()
          .map(edge -> OutboundEdge.from(edge.getStartTimeMillis(), edge.getEndTimeMillis()))
          .forEach(outboundEdges::add);
      outboundEdges.sort((o1, o2) -> (int) (o1.startTimeMillis - o2.startTimeMillis));
      // todo: Consider only those EXIT events that are CHILD_OF
      // todo: Filter for HTTP backends
      var entryApiBoundaryEventDuration =
          entryEvent.get().getEndTimeMillis() - entryEvent.get().getStartTimeMillis();
      var totalWaitTime = 0L;
      if (outboundEdges.size() > 0) {
        totalWaitTime = calculateTotalWaitTime(outboundEdges);
        ;
      }
      entryEvent
          .get()
          .getAttributes()
          .getAttributeMap()
          .put(
              EnrichedSpanConstants.API_INTERNAL_DURATION,
              AttributeValueCreator.create(
                  String.valueOf(entryApiBoundaryEventDuration - totalWaitTime)));
      // also put into metric map
      entryEvent
          .get()
          .getMetrics()
          .getMetricMap()
          .put(
              EnrichedSpanConstants.API_INTERNAL_DURATION,
              fastNewBuilder(MetricValue.Builder.class)
                  .setValue((double) (entryApiBoundaryEventDuration - totalWaitTime))
                  .build());
    }
  }

  private long calculateTotalWaitTime(List<OutboundEdge> outboundEdges) {
    long totalWait = 0;
    long startTime = outboundEdges.get(0).getStartTimeMillis();
    long endTime = outboundEdges.get(0).getEndTimeMillis();
    for (int i = 0; i < outboundEdges.size() - 1; i++) {
      var virtualCurrEdge = OutboundEdge.from(startTime, endTime);
      var lookaheadEdge = outboundEdges.get(i + 1);
      if (areSequential(virtualCurrEdge, lookaheadEdge)) {
        totalWait += virtualCurrEdge.getDuration();
        startTime = lookaheadEdge.getStartTimeMillis();
        endTime = lookaheadEdge.getEndTimeMillis();
      } else {
        startTime =
            Math.min(virtualCurrEdge.getStartTimeMillis(), lookaheadEdge.getStartTimeMillis());
        endTime = Math.max(virtualCurrEdge.getEndTimeMillis(), lookaheadEdge.getEndTimeMillis());
      }
    }
    totalWait += (endTime - startTime);
    return totalWait;
  }

  private boolean areSequential(OutboundEdge currEdge, OutboundEdge lookaheadEdge) {
    return lookaheadEdge.getStartTimeMillis() >= currEdge.getEndTimeMillis();
  }

  private static class OutboundEdge {

    private final long startTimeMillis;
    private final long endTimeMillis;
    private final long duration;

    OutboundEdge(long startTimeMillis, long endTimeMillis) {
      this.startTimeMillis = startTimeMillis;
      this.endTimeMillis = endTimeMillis;
      this.duration = endTimeMillis - startTimeMillis;
    }

    public static OutboundEdge from(long startTimeMillis, long endTimeMillis) {
      return new OutboundEdge(startTimeMillis, endTimeMillis);
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
