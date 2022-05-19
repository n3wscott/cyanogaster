# Cyanogaster

A transparent broker implementation for testing and development.

## Installing

```shell
ko apply -Bf ./config/saas/

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-br-defaults
  namespace: knative-eventing
data:
  default-br-config: |
    # This is the cluster-wide default broker channel.
    clusterDefault:
      brokerClass: GlassBroker
      delivery:
        retry: 5
        backoffPolicy: exponential
        backoffDelay: PT0.1S
EOF
```

## Testing

```shell
kubectl apply -f - << EOF
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  name: demo
  annotations:
    eventing.knative.dev/broker.class: GlassBroker
EOF
```

```shell
kubectl apply -f - << EOF
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: demo
spec:
  broker: demo
  filter:
    attributes:
      type: dev.chainguard.ingester.ingest.v1
  subscriber:
    uri: http://demo.demo-system.svc
EOF
```
