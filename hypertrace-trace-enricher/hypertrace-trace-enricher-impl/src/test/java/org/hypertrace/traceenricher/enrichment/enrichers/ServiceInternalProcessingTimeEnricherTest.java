package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.stream.Collectors.toList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.Enricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServiceInternalProcessingTimeEnricherTest extends AbstractAttributeEnricherTest {

  private final Enricher testCandidate = new ApiNodeInternalDurationEnricher();
  private StructuredTrace trace;

  @BeforeEach
  public void setup() throws IOException {
    Gson gson =
        new GsonBuilder()
            .serializeNulls()
            .registerTypeHierarchyAdapter(ByteBuffer.class, new ByteBufferTypeAdapter())
            .create();

    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("trace.json");
    trace = gson.fromJson(new FileReader(resource.getPath()),
        StructuredTrace.class);
  }

  @Test
  public void validateServiceInternalTimeAttributeInEntrySpans() {
    // this trace has 12 api nodes
    // api edges
    // 0 -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    // backend exit
    // 1 -> to redis 13 exit calls
    // 2 -> to mysql 1 exit call
    // for events parts of api_node 0, there should 12 exit calls
    // for events parts of api_node 1, there should be 13 exit calls
    // for events parts of api_node 2, there should be 1 exit calls
    // this trace has 4 services: [frontend, driver, customer, route]
    // frontend service has 1 api_entry span and that api_node has 12 exit calls [driver: 1,
    // customer: 1, route: 10]
    // setup
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
                        .containsKey(EnrichedSpanConstants.INTERNAL_SVC_LATENCY)));
  }

  @Test
  public void validateServiceInternalTimeValueInSpans() {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    var apiNodes = apiTraceGraph.getApiNodeList();
    List<Event> entryApiBoundaryEvents =
        apiNodes.stream().map(a -> a.getEntryApiBoundaryEvent().get()).collect(toList());
    // assert pre-conditions
//    Assertions.assertEquals(13, entryApiBoundaryEvents.size());
    // execute
//    testCandidate.enrichTrace(trace);
//    1613406996355, 1613406996653
//    1613406996653, 1613406996836
//    1613406996836, 1613406996898
//    1613406996836, 1613406996902
//    1613406996837, 1613406996909
//    1613406996899, 1613406996951
//    1613406996902, 1613406996932
//    1613406996909, 1613406996960
//    1613406996932, 1613406996979
//    1613406996951, 1613406996996
//    1613406996960, 1613406997014
//    1613406996980, 1613406997033
    //total wait time for frontend = (1613406996653 - 1613406996355) + (1613406996836 - 1613406996653) + (1613406997033 - 1613406996836) = 678ms
    testCandidate.enrichTrace(trace);
    var entryEventForFrontendSvc =
        getEntryEventsForService(entryApiBoundaryEvents, "frontend").get(0);
    apiNodes.get(0).getExitApiBoundaryEvents()
        .forEach(a -> System.out.println(a.getAttributes().getAttributeMap().get("http.url") + ", " + a.getStartTimeMillis() + ", " + a.getEndTimeMillis()));
    // total outbound edge duration = 1016ms
    // entry event duration = 678ms
    Assertions.assertEquals(
        "0",
        entryEventForFrontendSvc
            .getAttributes()
            .getAttributeMap()
            .get(EnrichedSpanConstants.INTERNAL_SVC_LATENCY)
            .getValue());
  }

  private static List<Event> getEntryEventsForService(
      List<Event> entryApiBoundaryEvents, String service) {
    return entryApiBoundaryEvents.stream()
        .filter(a -> a.getServiceName().equals(service))
        .collect(Collectors.toList());
  }

  private static String getEventDuration(Event event) {
    return String.valueOf(event.getMetrics().getMetricMap().get("Duration").getValue());
  }

  public static class ByteBufferTypeAdapter
      implements JsonDeserializer<ByteBuffer>, JsonSerializer<ByteBuffer> {

    @Override
    public ByteBuffer deserialize(
        JsonElement jsonElement, Type type, JsonDeserializationContext context) {
      return ByteBuffer.wrap(Base64.decodeBase64(jsonElement.getAsString()));
    }

    @Override
    public JsonElement serialize(ByteBuffer src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.encodeBase64String(src.array()));
    }
  }
}
