# Grafana Dashboard Setup for FlowPipeline Matching Data (Local Installation)

This guide helps you set up Grafana and Prometheus locally (without Docker) to visualize FlowPipeline matching data.

## Prerequisites

- FlowPipeline running with Prometheus exporter on port 9110
- Linux system (Debian/Ubuntu)

## Installation Steps

### Step 1: Install Prometheus

```bash
# Download Prometheus (check for latest version at https://prometheus.io/download/)
cd /tmp
wget https://github.com/prometheus/prometheus/releases/download/v2.48.0/prometheus-2.48.0.linux-amd64.tar.gz
tar xvfz prometheus-2.48.0.linux-amd64.tar.gz
sudo mv prometheus-2.48.0.linux-amd64 /opt/prometheus
```

Copy the configuration file:
```bash
sudo cp /root/Research-project/flowpipeline/grafana/prometheus.yml /opt/prometheus/
```

Create a systemd service (optional, for running as a service):
```bash
sudo tee /etc/systemd/system/prometheus.service > /dev/null <<EOF
[Unit]
Description=Prometheus
Wants=network-online.target
After=network-online.target

[Service]
User=root
ExecStart=/opt/prometheus/prometheus --config.file=/opt/prometheus/prometheus.yml --storage.tsdb.path=/opt/prometheus/data
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable prometheus
sudo systemctl start prometheus
```

Or run manually:
```bash
cd /opt/prometheus
./prometheus --config.file=prometheus.yml &
```

Verify Prometheus is running: http://localhost:9090

### Step 2: Install Grafana

```bash
# Add Grafana repository
sudo apt-get install -y software-properties-common
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -

# Install Grafana
sudo apt-get update
sudo apt-get install grafana

# Start Grafana
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

Verify Grafana is running: http://localhost:3000

### Step 3: Configure Grafana

1. **Login to Grafana**
   - Open http://localhost:3000
   - Username: `admin`
   - Password: `admin`
   - Change password when prompted

2. **Add Prometheus Data Source**
   - Go to Configuration → Data Sources
   - Click "Add data source"
   - Select "Prometheus"
   - Set URL to: `http://localhost:9090`
   - Click "Save & Test"

   **Or use the CLI:**
   ```bash
   # Copy the datasource config
   sudo cp /root/Research-project/flowpipeline/grafana/datasource.yml /etc/grafana/provisioning/datasources/
   sudo systemctl restart grafana-server
   ```

3. **Import the Dashboard**
   - Go to Dashboards → Import
   - Click "Upload JSON file"
   - Select: `/root/Research-project/flowpipeline/grafana/flowpipeline-dashboard.json`
   - Select "Prometheus" as the data source
   - Click "Import"

### Step 4: Start FlowPipeline

Make sure your FlowPipeline is running with the matching segment:

```bash
cd /root/Research-project/flowpipeline
./flowpipeline -config config.yml
```

## Quick Start (Manual Run)

If you don't want to install as services:

**Terminal 1 - Start Prometheus:**
```bash
cd /opt/prometheus
./prometheus --config.file=/root/Research-project/flowpipeline/grafana/prometheus.yml
```

**Terminal 2 - Start Grafana:**
```bash
sudo systemctl start grafana-server
# Or if installed from binary:
# /usr/sbin/grafana-server --config=/etc/grafana/grafana.ini
```

**Terminal 3 - Start FlowPipeline:**
```bash
cd /root/Research-project/flowpipeline
./flowpipeline -config config.yml
```

## Dashboard Features

The dashboard includes:

1. **Total Flow Traffic Rate** - Real-time traffic rate across all flows
2. **Total Kafka Messages Processed** - Counter of messages consumed
3. **Unique Flow Patterns** - Number of distinct flow patterns matching bad IPs
4. **Top 10 Source IPs by Traffic** - Pie chart showing top sources
5. **Traffic by Protocol** - Breakdown by protocol
6. **Top 10 Destination Ports** - Most active destination ports
7. **Flow Details Table** - Detailed view of top 20 flows by volume

## Troubleshooting

### Check if Prometheus is scraping FlowPipeline

Visit http://localhost:9090/targets - both targets should show "UP" status

### Verify FlowPipeline metrics are available

```bash
curl http://localhost:9110/flowdata
curl http://localhost:9110/metrics
```

### Check Grafana logs

```bash
sudo journalctl -u grafana-server -f
```

### Check Prometheus logs

```bash
sudo journalctl -u prometheus -f
```

### No data in Grafana

1. Ensure FlowPipeline is running and processing flows
2. Check matching segment is finding IPs (look for "MATCH FOUND!" in logs)
3. Verify Prometheus targets are UP
4. Check time range in Grafana (top right corner)

## Service Management

```bash
# Prometheus
sudo systemctl status prometheus
sudo systemctl stop prometheus
sudo systemctl restart prometheus

# Grafana
sudo systemctl status grafana-server
sudo systemctl stop grafana-server
sudo systemctl restart grafana-server
```

## File Locations

- Prometheus config: `/root/Research-project/flowpipeline/grafana/prometheus.yml`
- Grafana datasource: `/root/Research-project/flowpipeline/grafana/datasource.yml`
- Dashboard JSON: `/root/Research-project/flowpipeline/grafana/flowpipeline-dashboard.json`
- Grafana config: `/etc/grafana/grafana.ini`
- Grafana provisioning: `/etc/grafana/provisioning/`

## Customization

### Change Dashboard Refresh Rate

Edit the dashboard in Grafana UI or modify `"refresh": "10s"` in the JSON file.

### Add More Labels to Metrics

Update `labels` in `/root/Research-project/flowpipeline/config.yml`:
```yaml
- segment: prometheus
  config:
    endpoint: ":9110"
    labels: "SamplerAddress,Etype,DstPort,ProtoName,FlowDirection,SrcAddr,DstAddr,SrcAs,DstAs"
```

Then restart FlowPipeline and update dashboard queries accordingly.
