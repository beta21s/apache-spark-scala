apt install ntpdate
timedatectl set-timezone Asia/Ho_Chi_Minh
ntpdate -s ntp.ubuntu.com

kubeadm reset

kubeadm join 172.20.6.10:6443 --token ur0o96.mzvyoyg039jmfsh4 --discovery-token-ca-cert-hash sha256:d744e2828427e584646c8d5d29a0c392e5155d9b1b6eda3eed417e049e488e66
