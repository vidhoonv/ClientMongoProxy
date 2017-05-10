# Cursor Proxy Client side Tool for Mongo API Support in Azure CosmosDB

This tool can be used in the client side to provide cursor support for Mongo API in Azure CosmosDB.
It maintains a client side cursor map that maps each cursor Id to elements of pagination in Azure CosmosDB like Continuation token.
The client mongo app must connect to this proxy instead of connecting directly to remote CosmosDB endpoint.
This proxy maintains a client side cursor map that maps each cursor Id to elements of pagination in Azure CosmosDB like Continuation token.
Then, every request is forwarded by the proxy. Query and getMore requests/responses are parsed and used to populate the cursor map. 

## Getting Started

### Prerequisites

* Mongodb java driver 3.2.2

### Configuration

All configuration parameters are set via 'config' file in resources folder.
Here is the list of config parameters:

#### AccEndpoint 
Database account endpoint from Azure Portal

#### MongoPort
Mongo port from Azure Portal

#### ProxyPort
Local port on which the proxy client runs

###### Note
Make sure you connect the mongo client app to this port in the local machine so that requests are forwarded to remote endpoint correctly.

#### MaxNumberofCursors
Cursor map size before cleaning up old cursors

#### CursorExpiryAgeInMins
If cursor was created earlier than this duration, then it will be cleaned up during cleanup.


Here is a sample config file:

```
AccEndpoint=<<endpoint>>
MongoPort=10250
ProxyPort=10995
MaxNumberofCursors=1000
CursorExpiryAgeInMins=30
```

### SSL setup

Since the proxy is intended to run on the machine in which the Mongo client app is run, the connection from Mongo client app to proxy is not SSL enabled.
But since Azure CosmosDB only supports SSL enabled connections, all outgoing connections from proxy client to remote endpoint are SSL enabled.

### Cursor Map cleanup and cursor expiry

Since, the cursor map is maintained in local machine, we want to make sure it does not grow arbitrarily.
Hence, we have two configs:
* MaxNumberofCursors
* CursorExpiryAgeInMins

As soon as Cursor map hits MaxNumberofCursors elements, we start performing cleanup.
During this cleanup, if cursor was created earlier than CursorExpiryAgeInMins, then it is removed from cursor map.

### Running

This screenshot shows connecting to the proxy via Mongo shell and executing a query command:

![](http://i.imgur.com/UNb0ecF.png)


This screenshot shows the proxy output:

![](http://i.imgur.com/NuBAmVH.png)


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Vidhoon Viswanathan** - [vidhoonv](https://github.com/vidhoonv)