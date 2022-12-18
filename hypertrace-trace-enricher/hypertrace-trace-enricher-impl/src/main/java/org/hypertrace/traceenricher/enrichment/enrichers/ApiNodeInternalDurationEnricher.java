package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;

public class ApiNodeInternalDurationEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichTrace(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    List<ApiNode<Event>> apiNodes = apiTraceGraph.getApiNodeList();
    for(ApiNode<Event> apiNode : apiNodes) {
      Optional<Event> entryEvent = apiNode.getEntryApiBoundaryEvent();
      List<Event> exitEvents = apiNode.getExitApiBoundaryEvents();
      //todo: Consider only those EXIT events that are CHILD_OF
      //create a DAG of exit events
      long[][] dag = createDAG(exitEvents);
    }
  }

  private long[][] createDAG(List<Event> events) {
    //we take an extra sentinel node to make calculations easier. This is also the starting node of the graph
    int totalNodes = events.size() + 1;
    long[][] graph = new long[totalNodes][totalNodes];
    for(int i = 0; i < events.size(); i++) {
      Event currNode = events.get(i);
      for(int j = i + 1; j < events.size(); j++) {
        Event candidateNode = events.get(j);
        if (candidateNode.getStartTimeMillis() >= currNode.getEndTimeMillis()) {
          //currNode -> candidateNode
          graph[i][j] = candidateNode.getEndTimeMillis() - candidateNode.getStartTimeMillis();
        }
      }
    }
    return graph;
  }

}
