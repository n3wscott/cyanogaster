#!/usr/bin/env bash

# Assumes running kind cluster and registry.

KNATIVE_VERSION="${KNATIVE_VERSION:-1.4.0}"

# Eliminates the resources blocks in a release yaml
function resource_blaster() {
  local REPO="${1}"
  local FILE="${2}"

  curl -L -s "https://github.com/knative/${REPO}/releases/download/knative-v${KNATIVE_VERSION}/${FILE}" \
    | yq e 'del(.spec.template.spec.containers[]?.resources)' - \
    `# Filter out empty objects that come out as {} b/c kubectl barfs` \
    | grep -v '^{}$'
}

resource_blaster serving serving-crds.yaml | kubectl apply -f -
resource_blaster eventing eventing-crds.yaml | kubectl apply -f -
sleep 3 # Avoid the race creating CRDs then instantiating them...
resource_blaster serving serving-core.yaml | kubectl apply -f -
resource_blaster eventing eventing-core.yaml | kubectl apply -f -
resource_blaster net-kourier kourier.yaml | kubectl apply -f -
kubectl patch configmap/config-network \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"ingress.class":"kourier.ingress.networking.knative.dev"}}'

# Wait for Knative to be ready (or webhook will reject services)
for x in $(kubectl get deploy --namespace knative-serving -oname); do
  kubectl rollout status --timeout 5m --namespace knative-serving $x
done
for x in $(kubectl get deploy --namespace knative-eventing -oname); do
  kubectl rollout status --timeout 5m --namespace knative-eventing $x
done

while ! kubectl patch configmap/config-features \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"kubernetes.podspec-fieldref":"enabled","kubernetes.podspec-volumes-emptydir":"enabled","kubernetes.podspec-init-containers":"enabled","kubernetes.podspec-securitycontext":"enabled"}}'
do
    echo Waiting for webhook to be up.
    sleep 1
done

# Enable magic dns so we can interact with services from actions.
resource_blaster serving serving-default-domain.yaml | kubectl apply -f -

# Wait for the job to complete, so we can reliably use ksvc hostnames.
kubectl wait -n knative-serving --timeout=90s --for=condition=Complete jobs --all
