import json
import logging
from typing import Dict, Any, Optional, Tuple
import paramiko
import yaml
from dataclasses import dataclass
import sys
import re
from config_reader import load_config


def calculate_max_values(stats: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """Calculate maximum and minimum values across all edge nodes"""
    max_values = {
        'interfaces': {
            'rx_errors': 0.0,
            'rx_misses': 0.0,
            'tx_errors': 0.0
        },
        'cpu': {
            'usage': 0.0,
            'crypto': 0.0,
            'slowpath': 0.0,
            'intercore': 0.0
        },
        'flow_cache': {
            'micro_hit_rate': float('inf'),
            'mega_hit_rate': float('inf')
        }
    }

    # Only process node stats, not the entire stats dictionary
    node_stats = stats.get('nodes', {})
    
    for node_id, node_data in node_stats.items():
        # Skip nodes with errors
        if 'error' in node_data:
            continue

        # Check interface stats
        for interface in node_data.get('interfaces', {}).values():
            max_values['interfaces']['rx_errors'] = max(
                max_values['interfaces']['rx_errors'],
                interface.get('rx_errors', 0)
            )
            max_values['interfaces']['rx_misses'] = max(
                max_values['interfaces']['rx_misses'],
                interface.get('rx_misses', 0)
            )
            max_values['interfaces']['tx_errors'] = max(
                max_values['interfaces']['tx_errors'],
                interface.get('tx_errors', 0)
            )

        # Check CPU stats
        cpu_stats = node_data.get('performance', {}).get('cpu_stats', {})
        for core_stats in cpu_stats.values():
            for metric in ['usage', 'crypto', 'slowpath', 'intercore']:
                if core_stats.get(metric) is not None:
                    max_values['cpu'][metric] = max(
                        max_values['cpu'][metric],
                        core_stats.get(metric, 0)
                    )

        # Check flow cache stats
        flow_cache_stats = node_data.get('performance', {}).get('flow_cache_stats', {})
        for cache_type in ['micro_hit_rate', 'mega_hit_rate']:
            for core_hit_rate in flow_cache_stats.get(cache_type, {}).values():
                if core_hit_rate is not None:
                    max_values['flow_cache'][cache_type] = min(
                        max_values['flow_cache'][cache_type],
                        core_hit_rate
                    )

    return max_values

@dataclass
class EdgeCommands:
    INTERFACES = "get interfaces | json"
    PERFSTATS = "get dataplane perfstats {interval}"

def load_edge_node_config() -> Tuple[Dict[str, str], Dict[str, Dict]]:
    """
    Load edge node configuration from YAML file
    Returns:
        Tuple containing:
        - Dict mapping node IDs to IP addresses
        - Dict mapping edge cluster IDs to node configurations
    """
    try:
        with open('edge_node_config.yaml', 'r') as file:
            config = yaml.safe_load(file)
            return config['edge_nodes'], config['edge_clusters']
    except Exception as e:
        logging.error(f"Failed to load edge node configuration: {e}")
        sys.exit(1)

class NSXEdgeStatsCollector:
    def __init__(self, verbose: bool = False):
        self.logger = logging.getLogger(__name__)
        self.verbose = verbose
        
        # Load both configuration and credentials
        config, credentials = load_config()
        self.edge_node_ip_map = config['edge_nodes']
        self.edge_clusters = config['edge_clusters']
        self.edge_credentials = credentials['edge_nodes']
        
        self.ssh_clients = {}

        if self.verbose:
            self.logger.info("NSX Edge Stats Collector initialized")
            self.logger.info(f"Found {len(self.edge_node_ip_map)} edge nodes in configuration")

    def _is_timestamp_only(self, error_output: str) -> bool:
        """
        Check if the error output contains only a timestamp.
        
        Args:
            error_output (str): The error output to check
            
        Returns:
            bool: True if the error output only contains a timestamp, False otherwise
        """
        # Remove leading/trailing whitespace and empty lines
        error_output = error_output.strip()
        
        # Common timestamp patterns
        timestamp_patterns = [
            # Pattern for "Thu Jan 09 2025 UTC 15:19:08.539" format
            r'^[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{2}\s+\d{4}\s+UTC\s+\d{2}:\d{2}:\d{2}\.\d{3}$',
            # Add more patterns if needed for other timestamp formats
        ]
        
        for pattern in timestamp_patterns:
            if re.match(pattern, error_output):
                return True
                
        return False

    def _get_node_credentials(self, node_id: str) -> Dict[str, str]:
        """Get credentials for specific node, falling back to default if not found"""
        default_creds = self.edge_credentials['default']
        node_specific_creds = self.edge_credentials.get('nodes', {}).get(node_id)
        
        if node_specific_creds:
            if self.verbose:
                self.logger.info(f"Using specific credentials for node {node_id}")
            return node_specific_creds
        
        if self.verbose:
            self.logger.info(f"Using default credentials for node {node_id}")
        return default_creds

    def _connect_to_node(self, node_id: str) -> None:
        """Establish SSH connection to edge node"""
        if node_id not in self.edge_node_ip_map:
            raise ValueError(f"Unknown node ID: {node_id}")

        node_ip = self.edge_node_ip_map[node_id]
        credentials = self._get_node_credentials(node_id)
        
        if self.verbose:
            self.logger.info(f"Attempting to connect to node {node_id} ({node_ip})")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(
                node_ip,
                username=credentials['username'],
                password=credentials['password'],
                timeout=10
            )
            self.ssh_clients[node_id] = ssh
            self.logger.info(f"Connected to node {node_id} ({node_ip})")
        except Exception as e:
            self.logger.error(f"Failed to connect to node {node_id} ({node_ip}): {e}")
            raise

    def _close_connections(self) -> None:
        """Close all SSH connections"""
        if self.verbose:
            self.logger.info(f"Closing {len(self.ssh_clients)} SSH connections")

        for node_id, ssh in self.ssh_clients.items():
            try:
                ssh.close()
                self.logger.info(f"Disconnected from node {node_id}")
            except Exception as e:
                self.logger.warning(f"Error disconnecting from node {node_id}: {e}")
        self.ssh_clients.clear()

    def _execute_command(self, node_id: str, command: str) -> Dict[str, Any]:
        """Execute command and return parsed JSON output"""
        if self.verbose:
            self.logger.info(f"Executing command on node {node_id}: {command}")

        ssh = self.ssh_clients.get(node_id)
        if not ssh:
            raise RuntimeError(f"Not connected to node {node_id}")

        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            
            # Only log warning if error output is not just a timestamp
            if error and self.verbose and not self._is_timestamp_only(error):
                self.logger.warning(f"Command produced error output: {error}")
            
            return json.loads(output)
        except Exception as e:
            self.logger.error(f"Error executing command '{command}' on node {node_id}: {e}")
            return {}

    def collect_interface_stats(self, node_id: str) -> Dict[str, Dict[str, float]]:
        """Collect specified interface statistics for a node"""
        if self.verbose:
            self.logger.info(f"Collecting interface statistics for node {node_id}")

        interfaces_data = self._execute_command(node_id, EdgeCommands.INTERFACES)
        interface_stats = {}

        for port in interfaces_data.get('physical_ports', []):
            name = port.get('name')
            stats = port.get('stats', {})
            
            if name:
                interface_stats[name] = {
                    'rx_errors': float(stats.get('rx_errors', 0)),
                    'rx_misses': float(stats.get('rx_misses', 0)),
                    'tx_errors': float(stats.get('tx_errors', 0))
                }
                
                if self.verbose:
                    self.logger.info(f"Collected stats for interface {name} on node {node_id}")

        return interface_stats

    def collect_performance_stats(self, node_id: str, interval: int = 1) -> Dict[str, Any]:
        """Collect CPU and flow cache statistics for a node"""
        if self.verbose:
            self.logger.info(f"Collecting performance statistics for node {node_id}")

        command = EdgeCommands.PERFSTATS.format(interval=interval)
        perf_data = self._execute_command(node_id, command)
        
        if not perf_data or not isinstance(perf_data, list):
            if self.verbose:
                self.logger.warning(f"No performance data returned for node {node_id}")
            return {}

        stats = {
            'cpu_stats': {},
            'flow_cache_stats': {
                'micro_hit_rate': {},
                'mega_hit_rate': {}
            }
        }

        # Process CPU stats
        for section in perf_data:
            if 'CpuStats' in section:
                if self.verbose:
                    self.logger.info(f"Processing CPU stats for node {node_id}")
                for cpu in section['CpuStats']:
                    core = cpu.get('core')
                    if core is not None:
                        stats['cpu_stats'][core] = {
                            'usage': self._parse_value(cpu.get('usage', 'n/a')),
                            'rx': self._parse_value(cpu.get('rx', '0 pps')),
                            'tx': self._parse_value(cpu.get('tx', '0 pps')),
                            'crypto': self._parse_value(cpu.get('crypto', '0 pps')),
                            'slowpath': self._parse_value(cpu.get('slowpath', '0 pps')),
                            'intercore': self._parse_value(cpu.get('intercore', '0 pps'))
                        }

            # Process Flow Cache stats
            elif 'FlowCacheStats' in section:
                if self.verbose:
                    self.logger.info(f"Processing Flow Cache stats for node {node_id}")
                flow_stats = section['FlowCacheStats']
                # Map old names to new names
                type_mapping = {
                    'micro': 'micro_hit_rate',
                    'mega': 'mega_hit_rate'
                }
                for old_type, new_type in type_mapping.items():
                    if old_type in flow_stats:
                        for core_stats in flow_stats[old_type]:
                            core = core_stats.get('core')
                            hit_rate = core_stats.get('hit rate', 'n/a')
                            if core is not None:
                                stats['flow_cache_stats'][new_type][core] = self._parse_value(hit_rate)

        if self.verbose:
            self.logger.info(f"Completed performance stats collection for node {node_id}")
            
        return stats

    def _parse_value(self, value: str) -> Optional[float]:
        """Parse string values from CLI output to numbers"""
        if value == 'n/a':
            return None
        try:
            # Handle percentage values
            if isinstance(value, str) and '%' in value:
                return float(value.replace('%', ''))
            # Handle pps values
            elif isinstance(value, str) and 'pps' in value:
                return float(value.split()[0])
            return float(value)
        except (ValueError, TypeError):
            return None

    def collect_all_stats(self, interval: int = 1) -> Dict[str, Any]:
        """Collect all specified statistics from all edge nodes"""
        if self.verbose:
            self.logger.info("Starting collection of all edge node statistics")
            
        all_stats = {'nodes': {}}
        
        try:
            for node_id in self.edge_node_ip_map:
                if self.verbose:
                    self.logger.info(f"Processing node: {node_id}")
                    
                try:
                    self._connect_to_node(node_id)
                    all_stats['nodes'][node_id] = {
                        'interfaces': self.collect_interface_stats(node_id),
                        'performance': self.collect_performance_stats(node_id, interval)
                    }
                    
                    if self.verbose:
                        self.logger.info(f"Successfully collected all stats for node {node_id}")
                        
                except Exception as e:
                    self.logger.error(f"Failed to collect stats from node {node_id}: {e}")
                    all_stats['nodes'][node_id] = {"error": str(e)}
        finally:
            self._close_connections()

        # Calculate max values across all nodes
        all_stats['max_values'] = calculate_max_values(all_stats)
        
        if self.verbose:
            self.logger.info("Completed collection of all edge node statistics")
            self.logger.info(f"Processed {len(all_stats['nodes'])} nodes")
            
        return all_stats

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    collector = NSXEdgeStatsCollector(verbose=True)
    
    try:
        stats = collector.collect_all_stats()
        print(json.dumps(stats, indent=2))
    except Exception as e:
        logging.error(f"Failed to collect stats: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()