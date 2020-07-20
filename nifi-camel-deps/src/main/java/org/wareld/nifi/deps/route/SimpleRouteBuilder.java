package org.wareld.nifi.deps.route;

import org.apache.camel.builder.RouteBuilder;
import org.wareld.nifi.deps.processor.MyProcessor;

public class SimpleRouteBuilder extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("direct-vm:javain").process(new MyProcessor()).to("log:java.log?level=INFO").to("direct-vm:javaout");
    }
}
