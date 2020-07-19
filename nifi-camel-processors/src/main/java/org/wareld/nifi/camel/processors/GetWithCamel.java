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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.assimbly.connector.Connector;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Camel Consumer"})
@CapabilityDescription("Get messages from Apache Camel component (consumer)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class GetWithCamel extends AbstractProcessor {

  public static final PropertyDescriptor RETURN_HEADERS =
      new PropertyDescriptor.Builder()
          .name("RETURN_HEADERS")
          .displayName("Return Headers")
          .description("Return Camel exchange out headers into Nifi flowfile attributes")
          .required(true)
          .allowableValues("true", "false")
          .defaultValue("false")
          .build();

  public static final PropertyDescriptor FROM_URI =
      new PropertyDescriptor.Builder()
          .name("FROM_URI")
          .displayName("From URI")
          .description("From endpoint. Consumes messages with the specified Camel component")
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

  public static final PropertyDescriptor LOG_LEVEL =
      new PropertyDescriptor.Builder()
          .name("LOG_LEVEL")
          .displayName("LOG_LEVEL")
          .description("Set the log level")
          .required(true)
          .defaultValue("OFF")
          .allowableValues("OFF", "INFO", "WARN", "ERROR", "DEBUG", "TRACE")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final Relationship SUCCESS =
      new Relationship.Builder().name("SUCCESS").description("Succes relationship").build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private Connector connector = new org.assimbly.connector.impl.CamelConnector();

  private TreeMap<String, String> properties;

  private ConsumerTemplate template;

  private String input;

  private String flowId;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    getLogger().info("Init process..............................");

    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(RETURN_HEADERS);
    descriptors.add(FROM_URI);
    descriptors.add(ERROR_URI);
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
      template = connector.getConsumerTemplate();

    } catch (Exception e) {
      getLogger().error("Can't create Apache Camel endpoint.");
      e.printStackTrace();
    }
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {

    // Get the message from the Camel route
    Exchange exchange = template.receive("direct:nifi-" + flowId);

    if (exchange == null) {

      return;
    }

    FlowFile flowfile = session.create();

    if (context.getProperty(RETURN_HEADERS).getValue().equals("true")) {
      // Write the camel headers into attributes
      for (Map.Entry<String, Object> entry : exchange.getMessage().getHeaders().entrySet()) {
        flowfile =
            session.putAttribute(
                flowfile, String.format("camel.%s", entry.getKey()), entry.getValue().toString());
      }
    }

    // To write the results back out to flow file
    flowfile =
        session.write(
            flowfile,
            new OutputStreamCallback() {

              @Override
              public void process(OutputStream out) throws IOException {
                try {
                  out.write(exchange.getMessage().getBody(byte[].class));
                } catch (Exception e) {
                  out.write(exchange.getMessage().getBody(String.class).getBytes());
                }
              }
            });

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

    String fromURIProperty = context.getProperty(FROM_URI).getValue();
    String toURIProperty = "direct:nifi-" + flowId;
    String errorURIProperty = context.getProperty(ERROR_URI).getValue();
    final String logLevelProperty = context.getProperty(LOG_LEVEL).getValue();

    if (errorURIProperty == null || errorURIProperty.isEmpty()) {
      errorURIProperty =
          "log:GetWithCamel. + flowId + ?level=OFF&showAll=true&multiline=true&style=Fixed";
    }

    properties = new TreeMap<>();

    properties.put("id", flowId);
    properties.put("flow.name", "camelroute-" + flowId);
    properties.put("flow.type", "default");

    properties.put("flow.logLevel", logLevelProperty);
    properties.put("flow.offloading", "false");

    properties.put("from.uri", fromURIProperty);
    properties.put("to.1.uri", toURIProperty);
    properties.put("error.uri", errorURIProperty);

    properties.put("offramp.uri.list", "direct:flow=" + flowId + "endpoint=1");

    connector.setFlowConfiguration(properties);
  }
}
