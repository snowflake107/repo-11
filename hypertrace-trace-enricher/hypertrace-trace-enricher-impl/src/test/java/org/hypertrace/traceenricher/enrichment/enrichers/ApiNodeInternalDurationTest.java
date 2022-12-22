package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.stream.Collectors.toList;
import static org.hypertrace.traceenricher.enrichedspan.constants.utils.SpanUtils.getMetricValue;

import java.io.IOException;
import java.util.List;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.Enricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiNodeInternalDurationTest extends AbstractAttributeEnricherTest {

  private final Enricher testCandidate = new ApiNodeInternalDurationEnricher();
  private StructuredTrace trace;

  @BeforeEach
  public void setup() throws IOException {
    trace = TestUtils.readJSONStructuredTraceFromClasspath("trace.json");
  }

  @Test
  public void validateServiceInternalTimeAttributeInEntrySpans() {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    var apiNodes = apiTraceGraph.getApiNodeList();
    // Assert preconditions
    Assertions.assertEquals(13, apiNodes.size());
    apiNodes.forEach(
        apiNode -> Assertions.assertTrue(apiNode.getEntryApiBoundaryEvent().isPresent()));
    List<String> serviceNames =
        apiNodes.stream()
            .map(
                apiNode -> {
                  Assertions.assertTrue(apiNode.getEntryApiBoundaryEvent().isPresent());
                  return apiNode.getEntryApiBoundaryEvent().get().getServiceName();
                })
            .collect(toList());
    Assertions.assertTrue(serviceNames.contains("frontend"));
    Assertions.assertTrue(serviceNames.contains("driver"));
    Assertions.assertTrue(serviceNames.contains("customer"));
    Assertions.assertTrue(serviceNames.contains("route"));
    // execute
    testCandidate.enrichTrace(trace);
    // assertions: All entry spans should have this tag
    apiTraceGraph
        .getApiNodeList()
        .forEach(
            a ->
                Assertions.assertTrue(
                    a.getEntryApiBoundaryEvent()
                        .get()
                        .getAttributes()
                        .getAttributeMap()
                        .containsKey(EnrichedSpanConstants.API_INTERNAL_DURATION)));
  }

  @Test
  public void validateHotrodTraceForInternalDuration() {

    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    var apiNodes = apiTraceGraph.getApiNodeList();
    List<Event> entryApiBoundaryEvents =
        apiNodes.stream().map(a -> a.getEntryApiBoundaryEvent().get()).collect(toList());
    testCandidate.enrichTrace(trace);

    // This Hotrod trace comprises four services: frontend, driver, customer and route.
    // there are 13 exit calls from frontend to [driver, customer and route]. Below are the start
    // and end times of each such EXIT call.
    //    1613406996355, 1613406996653 -> HTTP HTTP GET /customer
    //    1613406996653, 1613406996836 -> driver GRPC driver.DriverService/FindNearest
    //    1613406996836, 1613406996898 -> route HTTP GET: /route
    //    1613406996836, 1613406996902 -> route HTTP GET: /route
    //    1613406996837, 1613406996909 -> route HTTP GET: /route
    //    1613406996899, 1613406996951 -> route HTTP GET: /route
    //    1613406996902, 1613406996932 -> route HTTP GET: /route
    //    1613406996909, 1613406996960 -> route HTTP GET: /route
    //    1613406996932, 1613406996979 -> route HTTP GET: /route
    //    1613406996951, 1613406996996 -> route HTTP GET: /route
    //    1613406996960, 1613406997014 -> route HTTP GET: /route
    //    1613406996980, 1613406997033 -> route HTTP GET: /route
    // calls to /customer and /FindNearest are sequential. The 10 calls to /route are made via a
    // thread pool and are parallel. So total wait
    // time is: (1613406996653 - 1613406996355) + (1613406996836 - 1613406996653) + (1613406997033 -
    // 1613406996836) = 678ms
    // total outbound edge duration = 678ms
    // entry event duration = 678ms
    Assertions.assertEquals(
        0d,
        getMetricValue(
            entryApiBoundaryEvents.get(0), EnrichedSpanConstants.API_INTERNAL_DURATION, -1));

    // there are 13 EXIT calls from driver to redis. Here's the start and end times of each:
    //    1613406996655, 1613406996672
    //    1613406996672, 1613406996681
    //    1613406996681, 1613406996694
    //    1613406996694, 1613406996724
    //    1613406996725, 1613406996731
    //    1613406996731, 1613406996736
    //    1613406996736, 1613406996745
    //    1613406996745, 1613406996752
    //    1613406996752, 1613406996780
    //    1613406996781, 1613406996792
    //    1613406996792, 1613406996808
    //    1613406996808, 1613406996819
    //    1613406996819, 1613406996834
    // All of these calls are sequential, and the total wait time is simply the sum of duration of
    // each span = 177ms
    // entry even duration = 180ms
    // wait time = 177ms
    Assertions.assertEquals(
        3d,
        getMetricValue(
            entryApiBoundaryEvents.get(1), EnrichedSpanConstants.API_INTERNAL_DURATION, -1));

    // there is 1 EXIT call from customer to the SQL DB. Here're the start and end times:
    // 1613406996356, 1613406996652
    // total wait time = 296ms
    // total duration of ENTRY span = 296ms
    Assertions.assertEquals(
        0d,
        getMetricValue(
            entryApiBoundaryEvents.get(2), EnrichedSpanConstants.API_INTERNAL_DURATION, -1));

    // All 10 ENTRY spans to ROUTE have no EXIT span. So all time is taken internally.
    for (int i = 3; i < apiNodes.size(); i++) {
      var apiNode = apiNodes.get(i);
      // ENTRY event
      var entryEvent = apiNode.getEntryApiBoundaryEvent().get();
      Assertions.assertEquals(
          getMetricValue(entryEvent, "Duration", -1),
          getMetricValue(entryEvent, EnrichedSpanConstants.API_INTERNAL_DURATION, -1));
    }
  }
}
