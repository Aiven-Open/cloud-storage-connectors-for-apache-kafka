package io.aiven.kafka.connect.common.integration.sink;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.UrlPattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

public class FaultyProxy {

    private FaultyProxy() {
        // do not instantiate
    }

    public static WireMockServer createFaultyProxy(SinkStorage<?, ?> storage, String topicName) {
        final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        String urlPathPattern = storage.getURLPathPattern(topicName);
        wireMockServer.start();
        wireMockServer.addStubMapping(WireMock.request(RequestMethod.ANY.getName(), UrlPattern.ANY)
                .willReturn(aResponse().proxiedFrom(storage.getEndpointURL()))
                .build());
        wireMockServer.addStubMapping(
                WireMock.request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                        .inScenario("temp-error")
                        .willSetStateTo("Error")
                        .willReturn(aResponse().withStatus(400))
                        .build());
        wireMockServer.addStubMapping(
                WireMock.request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                        .inScenario("temp-error")
                        .whenScenarioStateIs("Error")
                        .willReturn(aResponse().proxiedFrom(storage.getEndpointURL()))
                        .build());
        return wireMockServer;
    }
}
