# A GRPC Proxy for Kafka Producers/Consumers
A Kafka proxy that simplifies existing Kafka APIs by an additional layer of indirection and exposing a gRPC API. It aims to help in organizations where your clients may consist of multiple programming languages. 
In the long run, we plan to add functionality such as topic placement, cluster health discovery and disaster routing.
