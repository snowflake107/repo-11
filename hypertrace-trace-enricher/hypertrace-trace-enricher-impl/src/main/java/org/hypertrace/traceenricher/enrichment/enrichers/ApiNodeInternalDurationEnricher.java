package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.AttributeValue;
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
      List<OutboundEdge> outboundEdges = apiNode.getExitApiBoundaryEvents()
          .stream()
          .filter(
              event -> {
                Map<String, AttributeValue> enrichedAttributes =
                    event.getEnrichedAttributes().getAttributeMap();
                return enrichedAttributes.containsKey("BACKEND_PROTOCOL")
                    && enrichedAttributes.get("BACKEND_PROTOCOL").getValue().contains("HTTP");
              })
          .map(event -> OutboundEdge.from(event.getStartTimeMillis(), event.getEndTimeMillis()))
          .collect(Collectors.toList());
      apiTraceGraph.getOutboundEdgesForApiNode(apiNode)
          .stream()
          .map(edge -> OutboundEdge.from(edge.getStartTimeMillis(), edge.getEndTimeMillis()))
          .forEach(outboundEdges::add);
      outboundEdges.sort((o1, o2) -> (int) (o1.startTimeMillis - o2.startTimeMillis));
      //todo: Consider only those EXIT events that are CHILD_OF
      //todo: Filter for HTTP backends
      var entryApiBoundaryEventDuration =
          entryEvent.get().getEndTimeMillis() - entryEvent.get().getStartTimeMillis();
      var totalWaitTime = calculateTotalWaitTime(outboundEdges);
      entryEvent.get()
          .getAttributes()
          .getAttributeMap()
          .put(
              EnrichedSpanConstants.INTERNAL_SVC_LATENCY,
              AttributeValueCreator.create(
                  String.valueOf(
                      entryApiBoundaryEventDuration
                          - totalWaitTime)));
      // also put into metric map
      entryEvent.get()
          .getMetrics()
          .getMetricMap()
          .put(
              EnrichedSpanConstants.INTERNAL_SVC_LATENCY,
              fastNewBuilder(MetricValue.Builder.class)
                  .setValue(
                      (double) (entryApiBoundaryEventDuration - totalWaitTime))
                  .build());

    }
  }

  private long calculateTotalWaitTime(List<OutboundEdge> outboundEdgeList) {
    var firstExitEvent = outboundEdgeList.get(0);
    long startTime = firstExitEvent.getStartTimeMillis(), endTime = firstExitEvent.getEndTimeMillis(), totalDuration = 0, runningDuration =
        endTime - startTime;
    for (int i = 1; i < outboundEdgeList.size(); i++) {
      var currEvent = outboundEdgeList.get(i);
      var currStartTime = currEvent.getStartTimeMillis();
      var currEndTime = currEvent.getEndTimeMillis();
      var currDuration = currEndTime - currStartTime;
      //if the curr span starts before the prev span ends
      // [----------]
      //        [-------------------]
      if (currStartTime < endTime) {
        if (currDuration > runningDuration) {
          runningDuration = currDuration;
          endTime = currEndTime;
        }
      } else {
        // [------------]
        //                 [-------------------]
        totalDuration += runningDuration;
        endTime = currEndTime;
        runningDuration = currDuration;
      }
    }
    return totalDuration;
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
