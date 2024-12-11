# NSX Edge and ESXi Network Performance Monitoring Solution

## Table of Contents
- [Overview](#overview)
- [Main Goals](#main-goals)
- [Architecture](#architecture)
- [Data Collection Details](#data-collection-details)
- [Configuration](#configuration)
- [Metric Details](#metric-details)
- [Operational Details](#operational-details)

## Overview
This project implements an automated solution for collecting, processing, and monitoring network performance metrics from both NSX Edge nodes and ESXi hosts in a virtualized environment. The collected metrics are then published to vRealize Operations (vROps) for monitoring and analysis.

## Main Goals

### 1. Comprehensive Performance Monitoring
- Collect detailed performance metrics from NSX Edge nodes and ESXi hosts
- Monitor CPU utilization, interface statistics, and flow cache performance
- Track network-related thread activities and resource usage

### 2. Centralized Metrics Management
- Aggregate performance data from multiple sources
- Normalize and process metrics for consistent reporting
- Push consolidated metrics to vROps for centralized monitoring

### 3. Performance Threshold Monitoring
- Track CPU usage against defined thresholds
- Monitor interface errors and network flow statistics
- Identify potential performance bottlenecks

## Architecture

### Core Components

#### 1. Main Orchestrator (`getEdgeNodeStatsMainScript.py`)
- Central coordination script that manages the overall metrics collection and publishing process
- Implements the `StatsCollector` class which orchestrates:
  - Sequential collection of metrics from Edge nodes and ESXi hosts
  - Processing and normalization of collected data
  - Publishing metrics to vROps
  - Error handling and notification management

#### 2. Edge Node Collector (`getAllStatsFromEdgeNode.py`)
- Implements `NSXEdgeStatsCollector` class for collecting metrics from NSX Edge nodes
- Gathers:
  - CPU performance statistics
  - Interface metrics
  - Flow cache statistics
- Handles SSH connections and command execution on Edge nodes

#### 3. ESXi Collector (`getAllStatsFromESXi.py`)
- Implements `ESXiStatsCollector` class for collecting metrics from ESXi hosts
- Collects:
  - Network interface statistics (vmnic stats)
  - Thread performance metrics
  - EnsNetWorld statistics
- Manages SSH connections to ESXi hosts

#### 4. Support Modules
- `requestvROpsAccessToken.py`: vROps authentication
- `edge_node_config_reader.py`: Configuration file parsing
- `sendNotificationOnError.py`: Error notification system

## Data Collection Details

### Edge Node Collection
1. Connects to each Edge node via SSH
2. Executes `get interfaces | json` to collect interface statistics
3. Executes `get dataplane perfstats {interval}` to collect performance metrics
4. Processes and aggregates the data
5. Calculates maximum values across all nodes

### ESXi Collection
1. Connects to each ESXi host in the cluster via SSH
2. Executes `net-stats -i 1 -tW -A` to collect network statistics
3. Processes statistics for:
   - VMNIC poll world threads
   - EnsNetWorld TX/RX threads
4. Calculates maximum values across all hosts

## Configuration

### Edge Node Configuration File (`edge_node_config.yaml`)
```yaml
edge_nodes:
  "18b3dd22-2ba6-482a-80a3-eb90068dfb2d": "10.191.21.93"  # Edge Node UUID: IP address
  "e5b7ce1b-4210-46dd-a3f9-38bd1c71e332": "10.191.21.97"  # Edge Node UUID: IP address

edge_clusters:
  "2ebffbe1-8aca-46de-919a-cf606c15ed82":    # Edge Cluster UUID
    esxi_hosts:
      "esxi-01": "10.163.183.151"            # ESXi hostname: IP address
      "esxi-02": "10.163.183.152"            # ESXi hostname: IP address
```

Configuration Requirements:
- Edge Node UUIDs must be obtained from NSX Manager for the nodes to be monitored
- Edge Cluster UUID must match the NSX Edge Cluster UUID containing the monitored nodes
- ESXi hosts should include all possible hosts where the Edge nodes could run
- All IP addresses must be reachable from the monitoring server

### Requirements Configuration (`requirements.py`)
```python
requirement = {
    "nsx_manager": {
        "ip": "<nsx_manager_ip>",
        "credential": {
            "username": "admin",
            "password": "****"
        }
    },
    "nsx_edge_nodes": {
        "credential": {
            "username": "admin",
            "password": "****"
        }
    },
    "esxi_hosts": {
        "credential": {
            "username": "root",
            "password": "****"
        }
    },
    "vrops_instance": {
        "ip": "<vrops_ip>",
        "credential": {
            "username": "admin",
            "password": "****"
        },
        "adapterInstanceId": "<adapter_instance_id>"  # From vROPs UI: Inventory->Adapters
    }
}
```

## Metric Details

### Edge Node Metrics

#### 1. Interface Statistics
For each interface (fp-eth0 through fp-eth3):
- `rx_errors`: Number of receive errors
- `rx_misses`: Number of receive packet misses
- `tx_errors`: Number of transmission errors

#### 2. CPU Performance
For each CPU core:
- `usage`: CPU utilization percentage
- `rx`: Receive packet rate (packets per second)
- `tx`: Transmit packet rate (packets per second)
- `crypto`: Cryptographic operations rate
- `slowpath`: Slow path packet processing rate
- `intercore`: Inter-core communication rate

#### 3. Flow Cache Statistics
For each core:
- `micro_hit_rate`: Micro flow cache hit rate percentage
- `mega_hit_rate`: Mega flow cache hit rate percentage

### ESXi Metrics

#### 1. VMNIC Statistics
For each vmnic:
- `max_used`: Maximum CPU usage percentage across all threads
- `max_ready`: Maximum ready time percentage
- Thread-specific statistics:
  - `used`: CPU usage percentage per thread
  - `ready`: Ready time percentage per thread

#### 2. EnsNetWorld Statistics
Tracks network processing threads divided into:
- TX (Transmit) threads
- RX (Receive) threads

Each thread includes:
- `used`: CPU usage percentage
- `ready`: Ready time percentage

## Operational Details

### Thread Filtering
The ESXi collector implements thread filtering to focus on significant resource usage:
- Only threads with CPU usage above a configurable threshold are collected
- Default threshold is 2% CPU usage
- Filtering applies to both VMNIC and EnsNetWorld threads
- Reduces noise in metrics by excluding low-activity threads

### Logging System

#### Configuration
Both collectors implement comprehensive logging with:
- Standard output (console) logging
- File logging to 'edge_monitoring.log'
- Configurable verbose mode

#### Verbose Mode
Enable verbose logging by initializing collectors with:
```python
collector = NSXEdgeStatsCollector(verbose=True)
collector = ESXiStatsCollector(verbose=True)
```

### vROPs Integration

1. Adapter Instance ID
   - Found in vROPs UI under Inventory -> Adapters
   - Required for metric publishing and notifications
   - Must be from an active NSX-T adapter instance

2. Authentication
   - Uses token-based authentication
   - Tokens are managed by the `requestvROpsAccessToken.py` module

3. Error Handling
   - Failed collections trigger notifications in vROPs
   - Notifications include detailed error messages
   - Can be viewed in vROPs alerts and notifications panel

### Security Considerations

1. Credential Management
   - Credentials are stored in `requirements.py`
   - File should have restricted permissions
   - Consider using environment variables or a secure vault in production

2. Network Access
   - Monitoring server needs SSH access to Edge nodes and ESXi hosts
   - HTTPS access required for vROPs API
   - Firewall rules should be configured accordingly
  
# Supplementary Technical Documentation

## Detailed Collection Process

### Edge Node Collection Commands
1. Interface Statistics Command:
   ```bash
   get interfaces | json
   ```
   - Collects raw interface metrics
   - JSON format for easy parsing
   - Includes all physical ports

2. Performance Statistics Command:
   ```bash
   get dataplane perfstats 1
   ```
   - 1-second sampling interval (Configurable)
   - Includes CPU and flow cache metrics
   - Real-time performance data

### ESXi Collection Command
```bash
net-stats -i 1 -tW -A
```
Parameters:
- `-i 1`: 1-second sampling interval
- `-t`: Include thread statistics
- `-W`: Wide output format
- `-A`: All statistics

## Logging Examples

### Edge Node Collector Verbose Output
```
2024-12-11 19:17:25,952 - INFO - NSX Edge Stats Collector initialized
2024-12-11 19:17:25,953 - INFO - Found 2 edge nodes in configuration
2024-12-11 19:17:25,953 - INFO - Starting collection of all edge node statistics
2024-12-11 19:17:25,953 - INFO - Processing node: 18b3dd22-2ba6-482a-80a3-eb90068dfb2d
```

### ESXi Collector Verbose Output
```
2024-12-11 19:17:36,326 - INFO - ESXi Stats Collector initialized
2024-12-11 19:17:36,326 - INFO - Found 1 edge clusters in configuration
2024-12-11 19:17:36,326 - INFO - Found 2 ESXi hosts in cluster
```

## Thread Processing Details

### Thread Filtering Logic
```python
USAGE_THRESHOLD = 2.0  # Default 2% threshold

if thread_stats.get('used', 0) > USAGE_THRESHOLD:
    metrics.append({
        'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|{thread_name}|used',
        'timestamps': [timestamp],
        'data': [thread_usage]
    })
```

### Thread Types and Patterns

1. VMNIC Poll World Threads
   - Format: `vmnic<X>-pollWorld-<ID>-<ADDR>`
   - Example: `vmnic2-pollWorld-65-0x4301158d7d80`
   - Indicates NIC polling threads

2. EnsNetWorld Threads
   - Format: `EnsNetWorld-<GROUP>-<ID>`
   - Example: `EnsNetWorld-0-1`
   - Even IDs: RX threads
   - Odd IDs: TX threads

## vROPs Metric Structure

### Metric Key Format
```
EdgePerformanceMetrics|<Component>|<Host/Node>|<Metric>
```

Example metric paths:
```
EdgePerformanceMetrics|ESXi|esxi-01|vmnic2|threads_over_usage_threshold
EdgePerformanceMetrics|EdgeNodes|max_values|cpu_usage
```

## Error Notification System

### Notification Format
```python
suite_api_json = {
    "eventType": "NOTIFICATION",
    "cancelTimeUTC": 0,
    "severity": "WARNING",
    "keyIndicator": False,
    "managedExternally": False,
    "resourceId": adapter_instance_id,
    "message": error_message,
    "startTimeUTC": current_time_in_millis
}
```

### Common Error Scenarios
1. SSH Connection Failures
   ```
   Failed to connect to node {node_id} ({ip}): Connection timed out
   ```

2. Authentication Failures
   ```
   Authentication failed for host {host_id}
   ```

3. Command Execution Errors
   ```
   Error executing command '{command}' on node {node_id}: {error}
   ```

## Performance Considerations

### Timing
- Edge Node collection: ~2-3 seconds per node
- ESXi collection: ~1-2 seconds per host
- vROPs publication: ~1 second per batch

# Crontab Configuration for Edge Monitoring

## Basic Setup

### 1. Create a Wrapper Script
First, create a shell script to run the monitoring script with proper environment:

```bash
#!/bin/bash

# edge_monitoring_wrapper.sh
cd /path/to/ScriptsFolder
python3 getEdgeNodeStatsMainScript.py
```

Make the wrapper script executable:
```bash
chmod +x edge_monitoring_wrapper.sh
```

### 2. Add to Crontab
Edit crontab using:
```bash
crontab -e
```

Add the following line to run every minute:
```bash
* * * * * /path/to/edge_monitoring_wrapper.sh >> /path/to/cron.log 2>&1
```

## Detailed Setup Instructions

### 1. Environment Preparation

1. Verify Python Path:
```bash
which python3
```

2. Set up logging directory:
```bash
mkdir -p /var/log/edge_monitoring
chmod 755 /var/log/edge_monitoring
```

3. Create a comprehensive wrapper script:
```bash
#!/bin/bash

# edge_monitoring_wrapper.sh

# Set environment variables if needed
export PYTHONPATH=/path/to/ScriptsFolder

# Set working directory
cd /path/to/ScriptsFolder

# Log start time
echo "$(date): Starting Edge Monitoring Script" >> /var/log/edge_monitoring/cron.log

# Run script
python3 getEdgeNodeStatsMainScript.py >> /var/log/edge_monitoring/cron.log 2>&1

# Log completion
echo "$(date): Completed Edge Monitoring Script" >> /var/log/edge_monitoring/cron.log
```

### 2. Crontab Configuration Options

Basic (every minute):
```bash
* * * * * /path/to/edge_monitoring_wrapper.sh
```

With error logging:
```bash
* * * * * /path/to/edge_monitoring_wrapper.sh >> /var/log/edge_monitoring/cron.log 2>&1
```

With lock file to prevent overlapping:
```bash
* * * * * flock -n /tmp/edge_monitoring.lock /path/to/edge_monitoring_wrapper.sh
```

### 3. Log Rotation Configuration

Create `/etc/logrotate.d/edge_monitoring`:
```
/var/log/edge_monitoring/cron.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 root root
}
```

## Troubleshooting

### Common Issues

1. Script Not Running
- Check crontab is installed and running:
```bash
systemctl status cron
```
- Verify script permissions:
```bash
ls -l /path/to/edge_monitoring_wrapper.sh
```

2. Python Environment Issues
- Add complete path to python:
```bash
* * * * * /usr/bin/python3 /path/to/getEdgeNodeStatsMainScript.py
```

3. Log File Permissions
- Ensure proper permissions:
```bash
chown -R user:group /var/log/edge_monitoring
chmod 755 /var/log/edge_monitoring
chmod 644 /var/log/edge_monitoring/cron.log
```

### Monitoring Cron Execution

1. Check Cron Logs
```bash
grep CRON /var/log/syslog
```

2. Monitor Script Execution
```bash
tail -f /var/log/edge_monitoring/cron.log
```

### Best Practices

1. Test the wrapper script manually first:
```bash
./edge_monitoring_wrapper.sh
```

2. Use absolute paths in crontab entries

3. Set up log rotation to manage disk space

4. Add proper error handling in the wrapper script:
```bash
#!/bin/bash
set -e

# Log function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" >> /var/log/edge_monitoring/cron.log
}

# Error handling
handle_error() {
    log "Error occurred in edge monitoring script: $1"
    exit 1
}

trap 'handle_error "$BASH_COMMAND"' ERR

# Main execution
log "Starting Edge Monitoring Script"
cd /path/to/ScriptsFolder || handle_error "Failed to change directory"
python3 getEdgeNodeStatsMainScript.py || handle_error "Script execution failed"
log "Completed Edge Monitoring Script"
```

## Monitoring and Maintenance

### Regular Checks

1. Check log files for errors:
```bash
grep ERROR /var/log/edge_monitoring/cron.log
```

2. Monitor script duration:
```bash
grep "Starting\|Completed" /var/log/edge_monitoring/cron.log
```

3. Verify crontab entry:
```bash
crontab -l
```

### Maintenance Tasks

1. Regular log rotation:
```bash
logrotate -f /etc/logrotate.d/edge_monitoring
```

2. Check disk space:
```bash
du -sh /var/log/edge_monitoring/
```

3. Test script execution:
```bash
/path/to/edge_monitoring_wrapper.sh
```
