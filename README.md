![Docker](https://github.com/kubemq-io/kubemq-community/actions/workflows/docker-image.yml/badge.svg) ![goreleaser](https://github.com/kubemq-io/kubemq-community/actions/workflows/goreleaser.yml/badge.svg) ![Go Report Card](https://goreportcard.com/badge/github.com/kubemq-io/kubemq-community)
# What is KubeMQ Community?

KubeMQ Community is the community version of KubeMQ, the Kubernetive native message broker.
[More about KubeMQ](https://kubemq.io/)

# Installation

## Kubernetes

``` bash  
kubectl apply -f https://deploy.kubemq.io/community/  
```  

## Docker

Pull and run KubeMQ standalone docker container:

``` bash  
docker run -d -p 8080:8080 -p 50000:50000 -p 9090:9090 kubemq/kubemq-community:latest  
```  

## Binaries

1. Download the latest version of KubeMQ Community
   from [Releases](https://github.com/kubemq-io/kubemq-community/releases)
2. Unpack the downloaded archive
3. Run ```kubemq server```

# KubeMQ Connectors

|Name | Description | Github | |--|--|--| | KubeMQ Targets | Connect KubeMQ Message Broker with external systems and
cloud services |[KubeMQ Targets](https://github.com/kubemq-hub/kubemq-targets)|
| KubeMQ Sources | Connects external systems and cloud services with KubeMQ message queue broker
|[KubeMQ Sources](https://github.com/kubemq-hub/kubemq-sources)|
| KubeMQ Bridges | bridge, replicate, aggregate, and transform messages between KubeMQ
|[KubeMQ Bridges](https://github.com/kubemq-hub/kubemq-bridges)|

# KubeMQ CLI

KubeMQ community edition comes with built-in CLI tool for interaction with KubeMQ server. Check out the docs:

* [kubemq commands](https://github.com/kubemq-io/kubemq-community/blob/master/docs/cli/kubemq_commands.md)  - Execute
  Kubemq 'commands'
* [kubemq events](https://github.com/kubemq-io/kubemq-community/blob/master/docs/cli/kubemq_events.md)     - Execute
  Kubemq 'events' Pub/Sub commands
* [kubemq events_store](https://github.com/kubemq-io/kubemq-community/blob/master/docs/cli/kubemq_events_store.md)     -
  Execute Kubemq 'Events-Store' Pub/Sub commands
* [kubemq queries](https://github.com/kubemq-io/kubemq-community/blob/master/docs/cli/kubemq_queries.md)   - Execute
  Kubemq 'queries'
* [kubemq queues](https://github.com/kubemq-io/kubemq-community/blob/master/docs/cli/kubemq_queues.md)     - Execute
  Kubemq 'queues' commands

# KubeMQ SDKs

KubeMQ SDKs support list:

| SDK | Github   |  
|:----|:---|  
| C#    |  https://github.com/kubemq-io/kubemq-CSharp  |  
| Java    | https://github.com/kubemq-io/kubemq-Java |  
| Python    |  https://github.com/kubemq-io/kubemq-Python  |  
| Node    |  https://github.com/kubemq-io/kubemq-node |  
| Go    | https://github.com/kubemq-io/kubemq-go |  
| REST    |  https://postman.kubemq.io/ |  

## KubeMQ Cookbooks

| SDK | Github   |  
|:----|:---|  
| C#    |  https://github.com/kubemq-io/csharp-sdk-cookbook  |  
| Java    | https://github.com/kubemq-io/java-sdk-cookbook |  
| Python    |  https://github.com/kubemq-io/python-sdk-cookbook |  
| Node    |  https://github.com/kubemq-io/node-sdk-cookbook |  
| Go    | https://github.com/kubemq-io/go-sdk-cookbook|  

## Documatation

Visit our [Extensive KubeMQ Documentation](https://docs.kubemq.io/).

## Support

You can reach us at:

- [**Email**](mailto:support@kubemq.io)
- [**Slack**](https://kubemq.slack.com)
  - [Invitation](https://join.slack.com/t/kubemq/shared_invite/enQtNDk3NjE1Mjg1MDMwLThjMGFmYjU1NTVhZWRjZTRjYTIxM2E5MjA5ZDFkMWUyODI3YTlkOWY2MmYzNGIwZjY3OThlMzYxYjYwMTVmYWM)
- [**Open an issue**](https://github.com/kubemq-io/kubemq/issues)
