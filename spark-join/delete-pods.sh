kubectl get pod -A | grep Error | awk '{print $2 " --namespace=" $1}' | xargs -n 2 kubectl delete pod

kubectl get pod -A | grep Completed | awk '{print $2 " --namespace=" $1}' | xargs -n 2 kubectl delete pod

kubectl get pod -A | grep Evicted | awk '{print $2 " --namespace=" $1}' | xargs -n 2 kubectl delete pod

kubectl get pod -A | grep ContainerStatusUnknown | awk '{print $2 " --namespace=" $1}' | xargs -n 2 kubectl delete pod