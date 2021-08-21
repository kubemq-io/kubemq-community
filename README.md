# What is KubeMQ?
Enterprise-grade message broker native for Docker and Kubernetes. Delivered in a production-ready kubernetes cluster or in a standalone docker container or native binaries, and designed for any type of workload.  
KubeMQ is provided as a small, lightweight Docker container, designed for any workload and architecture running in Kubernetes or any other container orchestration system which support Docker.

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

1. Download the latest version of KubeMQ Community from [Releases](https://github.com/kubemq-io/kubemq-community/releases)
2. Unpack the downloaded archive
3. Run ```kubemq server```




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
- [**Slack**](https://kubemq.slack.com) - [Invitation](https://join.slack.com/t/kubemq/shared_invite/enQtNDk3NjE1Mjg1MDMwLThjMGFmYjU1NTVhZWRjZTRjYTIxM2E5MjA5ZDFkMWUyODI3YTlkOWY2MmYzNGIwZjY3OThlMzYxYjYwMTVmYWM)
- [**Open an issue**](https://github.com/kubemq-io/kubemq/issues)
