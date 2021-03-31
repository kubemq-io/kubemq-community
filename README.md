# Introduction

## What is KubeMQ?
Enterprise-grade message broker native for Docker and Kubernetes. Delivered in a production-ready cluster, and designed for any type of workload.
KubeMQ is provided as a small, lightweight Docker container, designed for any workload and architecture running in Kubernetes or any other container orchestration system which support Docker.

## Installation

Every installation method requires a KubeMQ key.
Please [register](https://account.kubemq.io/login/register) to obtain your KubeMQ key.

### Kubernetes

Install KubeMQ cluster on any Kubernetes cluster.
 
Step 1:

``` bash
kubectl apply -f https://deploy.kubemq.io/init
```

Step 2:

``` bash
kubectl apply -f https://deploy.kubemq.io/key/{{your key}}
```


### Docker

Pull and run KubeMQ standalone docker container:
``` bash
docker run -d -p 8080:8080 -p 50000:50000 -p 9090:9090 KEY={{yourkey}} kubemq/kubemq-standalone:latest
```

### Binaries

KubeMQ standalone binaries are available for Edge locations and for local development.

Steps:

1. Download the latest version of KubeMQ standalone from [Releases](https://github.com/kubemq-io/kubemq/releases)
2. Unpack the downloaded archive
3. Run ```kubemq -k {{your key}}``` (A key is needed for the first time only)

## KubeMQ SDKs
KubeMQ SDKs support list:

| SDK | Github   |
|:----|:---|
| C#    |  https://github.com/kubemq-io/kubemq-CSharp  |
| Jave    | https://github.com/kubemq-io/kubemq-Java |
| Python    |  https://github.com/kubemq-io/kubemq-Python  |
| Node    |  https://github.com/kubemq-io/kubemq-node |
| Go    | https://github.com/kubemq-io/kubemq-go |
| REST    |  https://postman.kubemq.io/ |

## Documatation

Visit our [Extensive KubeMQ Documentation](https://docs.kubemq.io/).

## Support

You can reach us at:
- [**Email**](mailto:support@kubemq.io)
- [**Slack**](https://kubemq.slack.com) - [Invitation](https://join.slack.com/t/kubemq/shared_invite/enQtNDk3NjE1Mjg1MDMwLThjMGFmYjU1NTVhZWRjZTRjYTIxM2E5MjA5ZDFkMWUyODI3YTlkOWY2MmYzNGIwZjY3OThlMzYxYjYwMTVmYWM) 
- [**By open an issue**](https://github.com/kubemq-io/kubemq/issues)
