wget https://github.com/prometheus/node_exporter/releases/download/v1.0.1/node_exporter-1.0.1.linux-amd64.tar.gz
tar xvzf node_exporter-1.0.1.linux-amd64.tar.gz
cp node_exporter-1.0.1.linux-amd64/node_exporter /usr/local/bin

cat << EOF > /lib/systemd/system/node_exporter.service
[Unit]
Description=Node Exporter
After=network-online.target

[Service]
User=root
Group=root
Type=simple
ExecStart=/usr/local/bin/node_exporter

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl start node_exporter
sudo systemctl status node_exporter
sudo systemctl enable node_exporter
sudo journalctl -u ignite -f
