# About

Nifi-Camel are some **experimental** processors for [Apache Nifi](http://nifi.apache.org/) that uses [Apache Camel](http://camel.apache.org/) to produce and consume messages. These processors are initially based on [Assimbly Connector](https://github.com/assimbly/connector), but it's forking quickly.

Use these experimental processors at your own risk, because you may really quickly write some integration mixture that won't comply anymore with Apache Nifi registry concepts, so use these experimental processors only and only if, you really understand and accept these limitations.

### Covered Apache Nifi/Camel Patterns
Camel Pattern                    | Nifi Processors        | Maturity Level        |
 ------------------------------- | :--------------------: | --------------------: |
Camel Exchange Pattern Out       | GetWithCamel           | Experimental          |
Camel Exchange Pattern InOnly    | PutWithCamel / InOnly  | Mature                |
Camel Exchange Pattern InOut     | PutWithCamel / InOut   | Mature                |
Camel Spring Java External Route | SpringContextProcessor | Unstable at this time |
Camel Spring DSL External Route  | SpringContextProcessor | Unstable at this time |
Monitoring Console               | Embedded Hawtio webapp | Experimental          |

### Installation

1. Build the NAR : mvn install
2. Put the NAR file in the Nifi lib directory : cp nifi-camel-nar/target/nifi-camel-nar-1.11.4.nar ..../nifi-1.11.4/lib/
3. For older installations of Nifi (before version 1.9) you need to restart Nifi.




### Usage

The PutWithCamel processor has 8 properties:

* Exchange Pattern: InOnly/InOut, InOut wait for bringing back the answer
* Return Body: true/false, if Camel body must be returned as Nifi flowfile content
* Return Headers: true/false, if Camel headers must be returned into Nifi flowfile attributes
* To URI: The URI of the Camel component
* Error URI: The URI of the Camel componet in case of an error
* Maximum Deliveries: Maximum of retries in case of an error 
* Delivery Delay: Delay between retries
* Log Level: The loglevel of the Camel route


The processor accepts dynamic properties prefixed with "camel."
![Alt text](doc/dynamic-properties.jpg?raw=true "Dynamic Properties")


Camel headers returned into Nifi attributes
![Alt text](doc/camel-headers.jpg?raw=true "Camel Headers")


### Usage

The GetWithCamel processor has 4 properties:

* Return Headers: true/false, if Camel headers must be returned into Nifi flowfile attributes
* To URI: The URI of the Camel component.
* Error URI: The URI of the Camel componet in case of an error
* Log Level: The loglevel of the Camel route


For the URI format of the camel component see [Camel's component reference](https://camel.apache.org/components/latest/). For 
example to use File uri: file://C/out
