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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.spring.SpringDataExchanger;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Processor} maintaining specfic Camel Spring context. Put your Camel Java
 * routes and/or your Camel DSL routes embedded into nifi-camel-deps component.
 */
@EventDriven
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Camel Embedded Context"})
@CapabilityDescription(
    "A Processor that supports sending and receiving data from application defined in "
        + "Spring Application Context via predefined in/out MessageChannels.")
public class CamelContext extends AbstractProcessor {
  private final Logger logger = LoggerFactory.getLogger(CamelContext.class);

  public static final PropertyDescriptor CTX_CONFIG_PATH =
      new PropertyDescriptor.Builder()
          .name("Application Context config path")
          .description(
              "The path to the Spring Application Context configuration file relative to the"
                  + " classpath")
          .defaultValue("applicationContext-camel.xml")
          .allowableValues("applicationContext-camel.xml")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  // ====

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description(
              "All FlowFiles that are successfully received from Spring Application Context are"
                  + " routed to this relationship")
          .build();
  public static final Relationship REL_FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description(
              "All FlowFiles that cannot be sent to Spring Application Context are routed to this"
                  + " relationship")
          .build();

  private static final Set<Relationship> relationships;

  private static final List<PropertyDescriptor> propertyDescriptors;

  // =======

  private volatile String applicationContextConfigFileName;

  private volatile SpringDataExchanger exchanger;

  static {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.add(CTX_CONFIG_PATH);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    relationships = Collections.unmodifiableSet(_relationships);
  }

  /** */
  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  /** */
  @OnScheduled
  public void initializeSpringContext(ProcessContext processContext) {
    this.applicationContextConfigFileName = processContext.getProperty(CTX_CONFIG_PATH).getValue();
    try {
      logger.debug(
          "Initializing Camel Spring Application Context defined in "
              + this.applicationContextConfigFileName);
      this.exchanger =
          CamelContextFactory.createSpringContextDelegate(this.applicationContextConfigFileName);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed while initializing Camel Spring Application Context", e);
    }
    logger.info(
        "Successfully initialized Camel Spring Application Context defined in "
            + this.applicationContextConfigFileName);
  }

  /**
   * Will close the 'exchanger' which in turn will close both Spring Application Context and the
   * ClassLoader that loaded it allowing new instance of Spring Application Context to be created
   * upon the next start (which may have an updated classpath and functionality) without restarting
   * NiFi.
   */
  @OnStopped
  public void closeSpringContext(ProcessContext processContext) {
    if (this.exchanger != null) {
      try {
        logger.debug(
            "Closing Camel Spring Application Context defined in "
                + this.applicationContextConfigFileName);
        this.exchanger.close();
        logger.info(
            "Successfully closed Camel Spring Application Context defined in "
                + this.applicationContextConfigFileName);
      } catch (IOException e) {
        getLogger().warn("Failed while closing Camel Spring Application Context", e);
      }
    }
  }

  /** */
  @Override
  protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
    SpringContextConfigValidator v = new SpringContextConfigValidator();
    return Collections.singletonList(
        v.validate(CTX_CONFIG_PATH.getName(), null, validationContext));
  }

  /** */
  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return propertyDescriptors;
  }

  /** */
  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {}

  /** */
  private Map<String, String> extractFlowFileAttributesFromMessageHeaders(
      Map<String, Object> messageHeaders) {
    Map<String, String> attributes = new HashMap<>();
    for (Entry<String, Object> entry : messageHeaders.entrySet()) {
      if (entry.getValue() instanceof String) {
        attributes.put(entry.getKey(), (String) entry.getValue());
      }
    }
    return attributes;
  }

  /** Extracts contents of the {@link FlowFile} to byte array. */
  private byte[] extractMessage(FlowFile flowFile, ProcessSession processSession) {
    final byte[] messageContent = new byte[(int) flowFile.getSize()];
    processSession.read(
        flowFile,
        new InputStreamCallback() {
          @Override
          public void process(final InputStream in) throws IOException {
            StreamUtils.fillBuffer(in, messageContent, true);
          }
        });
    return messageContent;
  }

  /** */
  static class SpringContextConfigValidator implements Validator {
    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
      String configPath = context.getProperty(CTX_CONFIG_PATH).getValue();

      StringBuilder invalidationMessageBuilder = new StringBuilder();
      if (configPath != null) {

        if (invalidationMessageBuilder.length() == 0 && !isConfigResolvable(configPath)) {
          invalidationMessageBuilder.append(
              "'Application Context config path' can not be located "
                  + "in the \"nifi-camel-deps\" path.");
        }
      }

      String invalidationMessage = invalidationMessageBuilder.toString();
      ValidationResult vResult =
          invalidationMessage.length() == 0
              ? new ValidationResult.Builder()
                  .subject(subject)
                  .input(input)
                  .explanation(
                      "Spring configuration '"
                          + configPath
                          + "' is resolvable "
                          + "n the \"nifi-camel-deps\" path.")
                  .valid(true)
                  .build()
              : new ValidationResult.Builder()
                  .subject(subject)
                  .input(input)
                  .explanation(
                      "Spring configuration '"
                          + configPath
                          + "' is NOT resolvable "
                          + "an the \"nifi-camel-deps\" path. Validation message: "
                          + invalidationMessage)
                  .valid(false)
                  .build();

      return vResult;
    }
  }

  /** */
  private static boolean isConfigResolvable(String configPath) {
    ClassLoader parentLoader = CamelContext.class.getClassLoader();
    boolean resolvable = false;
    try (URLClassLoader throwawayCl = new URLClassLoader(new URL[] {}, parentLoader)) {
      resolvable = throwawayCl.getResource(configPath) != null;
    } catch (IOException e) {
      // ignore since it can only happen on CL.close()
    }
    return resolvable;
  }
}
