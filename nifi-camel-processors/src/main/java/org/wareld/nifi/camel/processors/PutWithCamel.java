/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wareld.nifi.camel.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.assimbly.connector.Connector;
import org.assimbly.docconverter.DocConverter;

@Tags({"Camel Producer"})
@CapabilityDescription("Put messages to Apache Camel component (producer)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class PutWithCamel extends AbstractProcessor {

  public static final PropertyDescriptor EXCHANGE_PATTERN =
      new PropertyDescriptor.Builder()
          .name("EXCHANGE_PATTERN")
          .displayName("Exchange Pattern")
          .description("Camel Exchange Pattern to use")
          .required(true)
          .allowableValues("InOut", "InOnly")
          .defaultValue("InOut")
          .build();

  public static final PropertyDescriptor RETURN_BODY =
      new PropertyDescriptor.Builder()
          .name("RETURN_BODY")
          .displayName("Return Body")
          .description(
              "Return Camel exchange out body as Nifi flowfile content (work only with InOut"
                  + " pattern)")
          .required(true)
          .allowableValues("true", "false")
          .defaultValue("true")
          .build();

  public static final PropertyDescriptor RETURN_HEADERS =
      new PropertyDescriptor.Builder()
          .name("RETURN_HEADERS")
          .displayName("Return Headers")
          .description(
              "Return Camel exchange out headers into Nifi flowfile attributes (work only with"
                  + " InOut pattern)")
          .required(true)
          .allowableValues("true", "false")
          .defaultValue("false")
          .build();

  public static final PropertyDescriptor TO_URI =
      new PropertyDescriptor.Builder()
          .name("TO_URI")
          .displayName("To URI")
          .description("To endpoint. Producess message with the specified Camel component")
          .required(false)
          .addValidator(StandardValidators.URI_VALIDATOR)
          .build();

  public static final PropertyDescriptor ERROR_URI =
      new PropertyDescriptor.Builder()
          .name("ERROR_URI")
          .displayName("Error URI")
          .description("Error endpoint. Sends errors with the specified Camel component")
          .required(false)
          .addValidator(StandardValidators.URI_VALIDATOR)
          .build();

  public static final PropertyDescriptor MAXIMUM_REDELIVERIES =
      new PropertyDescriptor.Builder()
          .name("MAXIMUM_REDELIVERIES")
          .displayName("Maximum Redelivery")
          .description("Number of redeliveries on failure")
          .defaultValue("0")
          .required(true)
          .addValidator(StandardValidators.INTEGER_VALIDATOR)
          .build();

  public static final PropertyDescriptor REDELIVERY_DELAY =
      new PropertyDescriptor.Builder()
          .name("REDELIVERY_DELAY")
          .displayName("Redelivery Delay")
          .description("Delay in ms between redeliveries")
          .defaultValue("3000")
          .required(true)
          .addValidator(StandardValidators.INTEGER_VALIDATOR)
          .build();

  public static final PropertyDescriptor LOG_LEVEL =
      new PropertyDescriptor.Builder()
          .name("LOG_LEVEL")
          .displayName("LOG_LEVEL")
          .description("Set the log level")
          .required(true)
          .defaultValue("INFO")
          .allowableValues("OFF", "INFO", "WARN", "ERROR", "DEBUG", "TRACE")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
      final String propertyDescriptorName) {
    if (propertyDescriptorName.startsWith("camel.")) {
      String displayName;
      String description;
      displayName = propertyDescriptorName.substring(6);
      description =
          String.format(
              "Camel equivalent to <setHeader name=\"%s\"><simple>xxxx<simple></setHeader> (nifi"
                  + " expressions supported)",
              displayName);
      return new PropertyDescriptor.Builder()
          .name(propertyDescriptorName)
          .displayName(propertyDescriptorName)
          .description(description)
          .required(false)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .dynamic(true)
          .build();
    }
    return null;
  }

  public static final Relationship SUCCESS =
      new Relationship.Builder().name("SUCCESS").description("Succes relationship").build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private Connector connector = new org.assimbly.connector.impl.CamelConnector();

  private TreeMap<String, String> properties;

  private ProducerTemplate template;

  private String input;

  private String flowId;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    getLogger().info("Init process..............................");

    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(EXCHANGE_PATTERN);
    descriptors.add(RETURN_BODY);
    descriptors.add(RETURN_HEADERS);
    descriptors.add(TO_URI);
    descriptors.add(ERROR_URI);
    descriptors.add(MAXIMUM_REDELIVERIES);
    descriptors.add(REDELIVERY_DELAY);
    descriptors.add(LOG_LEVEL);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {

    // Use Assimbly Connector to manage Apache Camel (https://github.com/assimbly/connector)
    getLogger().info("Starting Apache Camel");

    // Start Apache camel
    try {
      startCamelConnector();
    } catch (Exception e2) {
      getLogger().error("Can't start Apache Camel.");
      e2.printStackTrace();
    }

    // Create a flow ID
    UUID uuid = UUID.randomUUID();
    flowId = context.getName() + uuid.toString();

    // configure the flow (Camel route)
    try {
      configureCamelFlow(context);
    } catch (Exception e1) {
      getLogger().error("Can't configure Apache Camel route.");
      e1.printStackTrace();
    }

    // start the flow (Camel route)
    try {
      connector.startFlow(flowId);
    } catch (Exception e1) {
      getLogger().error("Can't start Apache Camel.");
      e1.printStackTrace();
    }

    // Create the endpoint producer
    try {
      template = connector.getProducerTemplate();
      template.setDefaultEndpointUri("direct:nifi-" + flowId);
      template.start();

    } catch (Exception e) {
      getLogger().error("Can't create Apache Camel endpoint.");
      e.printStackTrace();
    }
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {

    final AtomicReference<byte[]> body = new AtomicReference<>(new byte[] {});
    final Map<String, Object> attributes = new ConcurrentHashMap<>();

    FlowFile flowfile = session.get();

    if (flowfile == null) {
      return;
    }

    Map<String, Object> headers = new HashMap<>();
    // Embed evaluated expression into headers map
    for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
      if (propertyDescriptor.isDynamic()) {
        PropertyValue myValue =
            context.newPropertyValue(context.getProperty(propertyDescriptor).getValue());
        headers.put(
            propertyDescriptor.getName().substring(6),
            myValue.evaluateAttributeExpressions(flowfile).getValue().toString());
      }
    }

    session.read(
        flowfile,
        new InputStreamCallback() {
          @Override
          public void process(InputStream in) throws IOException {
            try {

              // Convert flowFile to a string
              input = DocConverter.convertStreamToString(in);

              if (context.getProperty(EXCHANGE_PATTERN).getValue().equals("InOnly")) {
                // Enrich and process the exchange as InOnly
                Exchange exchange =
                    new DefaultExchange(connector.getContext(), ExchangePattern.InOnly);
                for (Map.Entry<String, Object> entry : headers.entrySet()) {
                  exchange.getIn().setHeader(entry.getKey(), entry.getValue().toString());
                }
                exchange.getIn().setBody(input);
                template.asyncSend("direct:nifi-" + flowId, exchange);
                getLogger()
                    .info(
                        String.format(
                            "%s InOnly message sent to \"direct:nifi-%s\"",
                            exchange.getExchangeId(), flowId));

              } else {
                // Enrich and process the exchange as InOut
                Exchange exchange =
                    template.request(
                        "direct:nifi-" + flowId,
                        new Processor() {
                          public void process(Exchange exchange) throws Exception {
                            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                              exchange
                                  .getIn()
                                  .setHeader(entry.getKey(), entry.getValue().toString());
                            }
                            exchange.getMessage().setHeaders(exchange.getIn().getHeaders());
                            exchange.getIn().setBody(input);
                            getLogger()
                                .info(
                                    String.format(
                                        "%s InOut message sent to \"direct:nifi-%s\"",
                                        exchange.getExchangeId(), flowId));
                          }
                        });

                // Retrieve out headers
                Map<String, Object> map = exchange.getMessage().getHeaders();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                  try {
                    if (!entry.getKey().startsWith("Assimbly")) {
                      attributes.put(
                          String.format("camel.%s", entry.getKey()), entry.getValue().toString());
                    }
                  } catch (Exception ex) {
                    // Some returned headers may not be string safe...
                    getLogger()
                        .debug(
                            String.format("Unconverted back camel header: \"%s\"", entry.getKey()));
                  }
                }
                // TODO: would it be possible to fail converting body into bytes ?
                body.set(exchange.getMessage().getBody(byte[].class));
              }

            } catch (Exception ex) {
              ex.printStackTrace();
              getLogger().error("Failed to send flowFile to Camel.");
            }
          }
        });

    if (context.getProperty(EXCHANGE_PATTERN).getValue().equals("InOut")) {

      if (context.getProperty(RETURN_HEADERS).getValue().equals("true")) {
        // Write the camel headers into attributes
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
          flowfile = session.putAttribute(flowfile, entry.getKey(), entry.getValue().toString());
        }
      }

      if (context.getProperty(RETURN_BODY).getValue().equals("true") && body.get() != null) {
        // To write the body/result back out into flow file
        flowfile =
            session.write(
                flowfile,
                new OutputStreamCallback() {
                  @Override
                  public void process(OutputStream out) throws IOException {
                    out.write(body.get());
                  }
                });
      }
    }

    session.transfer(flowfile, SUCCESS);
  }

  public void startCamelConnector() throws Exception {

    getLogger().info("Starting Apache Camel");

    // Start Camel context
    connector.start();
  }

  @OnStopped
  public void stopCamelConnector() throws Exception {

    getLogger().info("Stopping Apache Camel");

    connector.stopFlow(flowId);
    connector.stop();
    template.stop();
  }

  private void configureCamelFlow(final ProcessContext context) throws Exception {

    String fromURIProperty = "direct:nifi-" + flowId;
    String toURIProperty = context.getProperty(TO_URI).getValue();
    String errorURIProperty = context.getProperty(ERROR_URI).getValue();
    final String maximumRedeliveriesProperty = context.getProperty(MAXIMUM_REDELIVERIES).getValue();
    final String redeliveryDelayProperty = context.getProperty(REDELIVERY_DELAY).getValue();
    final String logLevelProperty = context.getProperty(LOG_LEVEL).getValue();

    if (errorURIProperty == null || errorURIProperty.isEmpty()) {
      errorURIProperty =
          "log:PutWithCamel. + flowId + ?level=OFF&showAll=true&multiline=true&style=Fixed";
    }

    properties = new TreeMap<>();

    properties.put("id", flowId);
    properties.put("flow.name", "camelroute-" + flowId);
    properties.put("flow.type", "default");

    properties.put("flow.maximumRedeliveries", maximumRedeliveriesProperty);
    properties.put("flow.redeliveryDelay", redeliveryDelayProperty);
    properties.put("flow.logLevel", logLevelProperty);
    properties.put("flow.offloading", "false");

    properties.put("from.uri", fromURIProperty);
    properties.put("to.1.uri", toURIProperty);
    properties.put("error.uri", errorURIProperty);

    properties.put("offramp.uri.list", "direct:flow=" + flowId + "endpoint=1");

    connector.setFlowConfiguration(properties);
  }
}
