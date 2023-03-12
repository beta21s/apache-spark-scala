kubectl -n kubernetes-dashboard create token admin-user --duration=488h > tolken.txt

# openssl s_client -servername remote.server.net -connect 172.20.6.10:30001 </dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' >/home/ubuntu/Documents/apache-spark-scala/spark-join/kube-apiserver-chain.pem
