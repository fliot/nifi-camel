# About

Nifi-Camel are some **experimental** processors for [Apache Nifi](http://nifi.apache.org/) that uses [Apache Camel](http://camel.apache.org/) to produce and consume messages. These processors are initially based on [Assimbly Connector](https://github.com/assimbly/connector), but it's forking quickly.

Use these experimental processors at your own risk, because you may really quickly write some integration mixture that won't comply anymore with Apache Nifi registry concepts, so use these experimental processors only and only if, you really understand and accept these limitations.


### Installation

1. Build the NAR : mvn install
2. Put the NAR file in the Nifi lib directory : cp nifi-camel-nar/target/nifi-camel-nar-1.11.4.nar ..../nifi-1.11.4/lib/
3. For older installations of Nifi (before version 1.9) you need to restart Nifi.


### Usage

All the Camel messaging patterns can be used, thanks to these 3 processors:
* **CamelContext** - autonomous processor, loading Java and/or DSL Camel routes. It must be deployed only once per Camel context to load.
* **GetWithCamel** - processor to consume Camel bind point (can be a component or end of route "direct-vm:output"...). This processor is downstream only.
* **PutWithCamel** - processor to submit to Camel bind point (can be a component or begining of route "direct-vm:input"...). This component can be used, in Camel terminology as InOut or InOnly, in Nifi terminology, with upstream, and with or without downstream.


### These examples below demonstrate messaging patterns implementation.

**Camel InOut**
![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-InOut.jpg?raw=true "Camel InOut")

**Camel InOnly-Out (loosely coupled)**
![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-InOnly-loosely-coupled-Out.jpg?raw=true "Camel InOnly-Out (loosely coupled)")

**Camel InOnly**
![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-InOnly.jpg?raw=true "Camel InOnly")

**Camel Out**
![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-Out.jpg?raw=true "Camel InOnly")



### Need to embed your own routes ?

* Adapt <em>nifi-camel-deps/pom.xml</em> to embed the Camel components you need.
* Code the Camel routes you want, in Java or in Camel DSL, make them available in <em>nifi-camel-deps/src/main/resources/somecontext.xml</em>
* Add the Spring context filename to <em>nifi-camel-processors/src/main/java/org/wareld/nifi/camel/processors/CamelContext.java</em> CTX_CONFIG_PATH allowed values.
* build and deploy.
* Add <em>CamelContext</em> processor, with selected application context, and "Run Schedule = 12 hours".

![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-context-2.jpg?raw=true "Camel Context")



### Use Camel Headers from Nifi Attributes, and Nifi Attributes for Camel Headers (and vice versa)

* expression support.
* from Nifi to Camel.
* from Camel to Nifi.

![Alt text](nifi-camel-processors/src/main/resources/docs/org.wareld.nifi.camel.processors.CamelContext/camel-headers.jpg?raw=true "Camel Context")


For the URI format of the camel component see [Camel's component reference](https://camel.apache.org/components/latest/). For 
example to use File uri: file://C/out
