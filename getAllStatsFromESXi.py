import paramiko
import re
import json
import logging
from typing import Dict, Any, Optional
import sys
import time
import requests
from dataclasses import dataclass
import copy

from config_reader import load_config



# Default stats structure for a host
DEFAULT_HOST_STATS = {
    'vmnic_stats': {
        'vmnic2': {'max_used': 0.0, 'max_ready': 0.0, 'threads': {}},
        'vmnic3': {'max_used': 0.0, 'max_ready': 0.0, 'threads': {}}
    }
}

@dataclass
class ESXiCommands:
    NET_STATS = "net-stats -i {interval} -tW -A"

class ESXiStatsCollector:
    def __init__(self, verbose: bool = False):
        self.logger = logging.getLogger(__name__)
        self.verbose = verbose
        
        # Load both configuration and credentials
        config, credentials = load_config()
        self.edge_node_ip_map = config['edge_nodes']
        self.edge_clusters = config['edge_clusters']
        self.esxi_credentials = credentials['esxi_hosts']
        
        self.ssh_clients = {}

    def _get_host_credentials(self, host_ip: str) -> Dict[str, str]:
        """Get credentials for specific host IP, falling back to default if not found"""
        default_creds = self.esxi_credentials['default']
        host_specific_creds = self.esxi_credentials.get('hosts', {}).get(host_ip)
        
        if host_specific_creds:
            if self.verbose:
                self.logger.info(f"Using specific credentials for host IP {host_ip}")
            return host_specific_creds
        
        if self.verbose:
            self.logger.info(f"Using default credentials for host IP {host_ip}")
        return default_creds

    def _connect_to_host(self, host_ip: str) -> None:
        """Establish SSH connection to ESXi host using IP address"""
        if self.verbose:
            self.logger.info(f"Attempting to connect to host at IP {host_ip}")

        credentials = self._get_host_credentials(host_ip)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(
                host_ip,
                username=credentials['username'],
                password=credentials['password'],
                timeout=10
            )
            self.ssh_clients[host_ip] = ssh
            self.logger.info(f"Connected to host at IP {host_ip}")
        except Exception as e:
            self.logger.error(f"Failed to connect to host at IP {host_ip}: {e}")
            raise

    def _close_connections(self) -> None:
        """Close all SSH connections"""
        if self.verbose:
            self.logger.info(f"Closing {len(self.ssh_clients)} SSH connections")

        for host_ip, ssh in self.ssh_clients.items():
            try:
                ssh.close()
                self.logger.info(f"Disconnected from host at IP {host_ip}")
            except Exception as e:
                self.logger.warning(f"Error disconnecting from host at IP {host_ip}: {e}")
        self.ssh_clients.clear()

    def _execute_command(self, host_ip: str, command: str) -> Dict[str, Any]:
        """Execute command and return parsed JSON output"""
        if self.verbose:
            self.logger.info(f"Executing command on host at IP {host_ip}: {command}")

        ssh = self.ssh_clients.get(host_ip)
        if not ssh:
            raise RuntimeError(f"Not connected to host at IP {host_ip}")

        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            
            if error and self.verbose:
                self.logger.warning(f"Command produced error output: {error}")
                
            return json.loads(output)
        except Exception as e:
            self.logger.error(f"Error executing command '{command}' on host at IP {host_ip}: {e}")
            return {}

    def _process_vmnic_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Process and organize vmnic statistics"""
        if self.verbose:
            self.logger.info("Processing vmnic and EnsNetWorld statistics")
            
        USAGE_THRESHOLD = 2.0  # Only collect stats above this threshold
        
        # Initialize stats structure with default vmnic entries
        vmnic_stats = {
            'vmnic2': {
                'max_used': 0.0,
                'max_ready': 0.0,
                'threads': {}
            },
            'vmnic3': {
                'max_used': 0.0,
                'max_ready': 0.0,
                'threads': {}
            }
        }

        # Initialize EnsNetWorld stats
        ens_stats = {
            'max_used': 0.0,
            'max_ready': 0.0,
            'tx': {'threads': {}},
            'rx': {'threads': {}}
        }
        
        has_ens_data = False

        # Process entries in the 'sys' section
        for sys_id, sys_data in stats_data.get('stats', [{}])[0].get('sys', {}).items():
            if 'name' not in sys_data or 'used' not in sys_data:
                continue

            thread_used = float(sys_data.get('used', 0))
            thread_ready = float(sys_data.get('ready', 0))
            
            # Skip threads below threshold
            if thread_used <= USAGE_THRESHOLD:
                continue

            thread_name = sys_data['name']
                    
            # Process EnsNetWorld threads
            if 'EnsNetWorld' in thread_name:
                match = re.search(r'EnsNetWorld-\d+-(\d+)', thread_name)
                if match:
                    if self.verbose:
                        self.logger.info(f"Found EnsNetWorld thread: {thread_name} with usage {thread_used}")
                    
                    thread_num = int(match.group(1))
                    thread_type = 'tx' if thread_num % 2 == 1 else 'rx'
                    
                    ens_stats[thread_type]['threads'][thread_name] = {
                        'used': thread_used,
                        'ready': thread_ready
                    }
                    
                    ens_stats['max_used'] = max(
                        ens_stats['max_used'],
                        thread_used
                    )
                    ens_stats['max_ready'] = max(
                        ens_stats['max_ready'],
                        thread_ready
                    )
                    
                    has_ens_data = True
                    
                    if self.verbose:
                        self.logger.info(f"Added EnsNetWorld {thread_type} stats: {thread_name}, used={thread_used}")
                    
            # Process pollWorld threads
            elif 'pollWorld' in thread_name:
                vmnic_match = re.search(r'(vmnic\d+)-pollWorld', thread_name)
                if vmnic_match:
                    vmnic_name = vmnic_match.group(1)
                    
                    vmnic_stats[vmnic_name]['threads'][thread_name] = {
                        'used': thread_used,
                        'ready': thread_ready
                    }
                    
                    vmnic_stats[vmnic_name]['max_used'] = max(
                        vmnic_stats[vmnic_name]['max_used'],
                        thread_used
                    )
                    vmnic_stats[vmnic_name]['max_ready'] = max(
                        vmnic_stats[vmnic_name]['max_ready'],
                        thread_ready
                    )
                    
                    if self.verbose:
                        self.logger.info(f"Added pollWorld stats for {vmnic_name}: {thread_name}, used={thread_used}")

        # Add EnsNetWorld stats if any were found
        if has_ens_data:
            vmnic_stats['ens'] = ens_stats
            if self.verbose:
                self.logger.info(f"Added EnsNetWorld stats: tx={len(ens_stats['tx']['threads'])} threads, rx={len(ens_stats['rx']['threads'])} threads")

        if self.verbose:
            self.logger.info(f"Processed stats for {len(vmnic_stats)} vmnics/types")
            if 'ens' in vmnic_stats:
                self.logger.info(f"Found EnsNetWorld threads: {len(ens_stats['tx']['threads'])} tx, {len(ens_stats['rx']['threads'])} rx")

        return vmnic_stats

    def collect_cluster_stats(self, cluster_id: str) -> Dict[str, Any]:
        """Collect statistics for all ESXi hosts in a cluster"""
        if self.verbose:
            self.logger.info(f"Collecting stats for cluster: {cluster_id}")

        cluster_stats = {
            'hosts': {},
            'max_values': {
                'used': 0.0,
                'ready': 0.0
            }
        }

        if cluster_id not in self.edge_clusters:
            self.logger.error(f"Cluster ID {cluster_id} not found")
            return cluster_stats
            
        cluster_data = self.edge_clusters[cluster_id]
        esxi_hosts = cluster_data.get('esxi_hosts', {})
        
        if self.verbose:
            self.logger.info(f"Found {len(esxi_hosts)} ESXi hosts in cluster")

        try:
            # Now the key is the host_ip and the value is the vrops_id
            for host_ip, vrops_id in esxi_hosts.items():
                try:
                    if self.verbose:
                        self.logger.info(f"Processing host at IP {host_ip} (vROPs ID: {vrops_id})")

                    self._connect_to_host(host_ip)
                    
                    command = ESXiCommands.NET_STATS.format(interval=1)
                    stats_data = self._execute_command(host_ip, command)
                    
                    if not stats_data:
                        if self.verbose:
                            self.logger.warning(f"No stats data returned from host at IP {host_ip}")
                        stats = copy.deepcopy(DEFAULT_HOST_STATS)
                        stats['vrops_id'] = vrops_id
                        cluster_stats['hosts'][host_ip] = stats
                        continue

                    # Process stats
                    vmnic_stats = self._process_vmnic_stats(stats_data)
                    
                    if vmnic_stats:
                        cluster_stats['hosts'][host_ip] = {
                            'vmnic_stats': vmnic_stats,
                            'vrops_id': vrops_id
                        }

                        # Update cluster max values
                        for vmnic_data in vmnic_stats.values():
                            cluster_stats['max_values']['used'] = max(
                                cluster_stats['max_values']['used'],
                                vmnic_data['max_used']
                            )
                            cluster_stats['max_values']['ready'] = max(
                                cluster_stats['max_values']['ready'],
                                vmnic_data['max_ready']
                            )
                    else:
                        if self.verbose:
                            self.logger.warning(f"No active vmnic stats found for host at IP {host_ip}")
                        stats = copy.deepcopy(DEFAULT_HOST_STATS)
                        stats['vrops_id'] = vrops_id
                        cluster_stats['hosts'][host_ip] = stats
                    
                except Exception as e:
                    self.logger.error(f"Failed to collect stats from ESXi host at IP {host_ip}: {e}")
                    stats = copy.deepcopy(DEFAULT_HOST_STATS)
                    stats['vrops_id'] = vrops_id
                    cluster_stats['hosts'][host_ip] = stats
                finally:
                    if host_ip in self.ssh_clients:
                        self.ssh_clients[host_ip].close()
                        if self.verbose:
                            self.logger.info(f"Closed connection to host at IP {host_ip}")
                    
        except Exception as e:
            self.logger.error(f"Error collecting cluster stats: {e}")
        finally:
            self._close_connections()
            
        if self.verbose:
            self.logger.info(f"Completed stats collection for cluster {cluster_id}")
            self.logger.info(f"Collected stats for {len(cluster_stats['hosts'])} hosts")
            self.logger.info(f"Cluster max values: used={cluster_stats['max_values']['used']}, ready={cluster_stats['max_values']['ready']}")
            
        return cluster_stats

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    collector = ESXiStatsCollector(verbose=True)
    
    try:
        first_cluster = next(iter(collector.edge_clusters))
        stats = collector.collect_cluster_stats(first_cluster)
        print(json.dumps(stats, indent=2))
    except Exception as e:
        logging.error(f"Failed to collect stats: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()