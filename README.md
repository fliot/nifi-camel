# About

Nifi-Camel is an **experimental** processor for [Apache Nifi](http://nifi.apache.org/) that uses [Apache Camel](http://camel.apache.org/) to produce and consume messages.

The processor is initially based on [Assimbly Connector](https://github.com/assimbly/connector), but is forking quickly.

### Installation

1. Build the nar : mvn install
2. Put the NAR file in the lib directory of Nifi : cp nifi-camel-nar/target/nifi-camel-nar-1.11.4.nar ..../nifi-1.11.4/lib/
3. For older installations of Nifi (before version 1.9) you need to restart.

### Usage

The ProduceWithCamel processor has 5 properties:

* To URI: The URI of the Camel component.
* Error URI: The URI of the Camel componet in case of an error
* Maximum Deliveries: Maximum of retries in case of an error 
* Delivery Delay: Delay between retries
* Log Level: The loglevel of the Camel route
* Return Headers: true/false, if Camel headers must be returned into Nifi attributes


The processor accepts dynamic properties prefixed with "camel."
![Alt text](doc/dynamic-properties.jpg?raw=true "Dynamic Properties")


Camel headers returned into Nifi attributes
![Alt text](doc/camel-headers.jpg?raw=true "Camel Headers")


### Usage

The ConsumeWithCamel processor has 3 properties:

* To URI: The URI of the Camel component.
* Error URI: The URI of the Camel componet in case of an error
* Log Level: The loglevel of the Camel route


For the URI format of the camel component see [Camel's component reference](https://camel.apache.org/components/latest/). For 
example to use File uri: file://C/out
