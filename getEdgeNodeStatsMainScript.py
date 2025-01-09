import json
import logging
from typing import Dict, Any, Optional
import sys
import time
import requests
from dataclasses import dataclass
import copy

import requestvROpsAccessToken as accessTokenProvider
import sendNotificationOnError as notify
from getAllStatsFromEdgeNode import NSXEdgeStatsCollector
from getAllStatsFromESXi import ESXiStatsCollector
from config_reader import load_config

# Default structures for missing or failed collections
DEFAULT_EDGE_NODE_STATS = {
    'performance': {
        'cpu_stats': {
            '0': {'usage': 0, 'rx': 0, 'tx': 0, 'crypto': 0, 'slowpath': 0, 'intercore': 0},
            '1': {'usage': 0, 'rx': 0, 'tx': 0, 'crypto': 0, 'slowpath': 0, 'intercore': 0},
            '2': {'usage': 0, 'rx': 0, 'tx': 0, 'crypto': 0, 'slowpath': 0, 'intercore': 0}
        },
        'flow_cache_stats': {
            'micro_hit_rate': {'0': 0, '1': 0, '2': 0},
            'mega_hit_rate': {'0': 0, '1': 0, '2': 0}
        }
    },
    'interfaces': {
        'fp-eth0': {'rx_errors': 0, 'rx_misses': 0, 'tx_errors': 0},
        'fp-eth1': {'rx_errors': 0, 'rx_misses': 0, 'tx_errors': 0},
        'fp-eth2': {'rx_errors': 0, 'rx_misses': 0, 'tx_errors': 0},
        'fp-eth3': {'rx_errors': 0, 'rx_misses': 0, 'tx_errors': 0}
    }
}

DEFAULT_ESXI_STATS = {
    'hosts': {},  # Will be populated dynamically as hosts are discovered
    'max_values': {
        'used': 0,
        'ready': 0
    }
}

class StatsCollector:
     def __init__(self, verbose: bool = False, usage_threshold: float = 85.0):
        self.logger = logging.getLogger(__name__)
        self.verbose = verbose
        self.usage_threshold = usage_threshold
        
        # Load both configuration and credentials
        self.config, self.credentials = load_config()
        
        # Update attribute assignments
        self.edge_node_ip_map = self.config['edge_nodes']
        self.edge_clusters = self.config['edge_clusters']
        self.vrops_config = {
            'ip': self.config['vrops_instance']['ip'],
            'adapterInstanceId': self.config['vrops_instance']['adapter_instance_id']
        }
        
        # Access token remains the same
        self.access_token = accessTokenProvider.vROpsAccessToken
        
        # Update credential references
        self.edge_credentials = self.credentials['edge_nodes']
        self.esxi_credentials = self.credentials['esxi_hosts']
        
        if self.verbose:
            self.logger.info("StatsCollector initialized with new configuration format")

     def _get_vrops_resource_map(self, adapter_kind: str, resource_kind: str) -> Dict[str, str]:
        """Get vROps resource ID mapping"""
        url = (f"https://{self.vrops_config['ip']}/suite-api/api/resources"
               f"?adapterInstanceId={self.vrops_config['adapterInstanceId']}"
               f"&adapterKind={adapter_kind}"
               f"&resourceKind={resource_kind}"
               f"&resourceStatus=DATA_RECEIVING"
               f"&_no_links=true")
        
        headers = {
            "accept": "application/json",
            "Authorization": f"vRealizeOpsToken {self.access_token}"
        }
        
        try:
            response = requests.get(url, headers=headers, verify=False)
            response.raise_for_status()
            resources = response.json().get('resourceList', [])
            
            resource_map = {}
            for resource in resources:
                resource_id = None
                for identifier in resource['resourceKey']['resourceIdentifiers']:
                    if identifier['identifierType']['name'] == 'ID':
                        resource_id = identifier['value']
                        break
                if resource_id:
                    resource_map[resource_id] = resource['identifier']
            
            return resource_map
        except Exception as e:
            self.logger.error(f"Failed to get vROps resource mapping: {e}")
            return {}

     def _publish_to_vrops(self, metrics: Dict[str, Any]) -> bool:
        """Publish metrics to vROps"""
        url = (f"https://{self.vrops_config['ip']}/suite-api/api/resources/stats"
               f"?disableAnalyticsProcessing=false&_no_links=true")
        
        headers = {
            "Content-Type": "application/json",
            "accept": "*/*",
            "Authorization": f"vRealizeOpsToken {self.access_token}"
        }

        try:
            response = requests.post(url, json=metrics, headers=headers, verify=False)
            response.raise_for_status()
            self.logger.info("Successfully published metrics to vROps")
            return True
        except Exception as e:
            self.logger.error(f"Failed to publish metrics: {e}")
            return False


     def _process_edge_stats(self, stats: Dict[str, Any], timestamp: int) -> list:
        """Process Edge Node stats into vROps format"""
        metrics = []
        
        # CPU Stats
        cpu_stats = stats.get('performance', {}).get('cpu_stats', {})
        for core, core_stats in cpu_stats.items():
            for stat_name, value in core_stats.items():
                if value is not None:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|CPU_Stats|Cores:{core}|{stat_name.upper()}',
                        'timestamps': [timestamp],
                        'data': [value]
                    })

        # Flow Cache Stats
        flow_stats = stats.get('performance', {}).get('flow_cache_stats', {})
        for cache_type in ['micro_hit_rate', 'mega_hit_rate']:
            for core, hit_rate in flow_stats.get(cache_type, {}).items():
                if hit_rate is not None and hit_rate > 0:  # Only include non-zero values
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|Flow_Cache_Stats|{cache_type}|Core:{core}',
                        'timestamps': [timestamp],
                        'data': [hit_rate]
                    })

        # Interface Stats
        for interface, interface_stats in stats.get('interfaces', {}).items():
            for stat_name, value in interface_stats.items():
                if value > 0:  # Only include non-zero values
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|PhysicalPorts:{interface}|{stat_name.upper()}',
                        'timestamps': [timestamp],
                        'data': [value]
                    })

        return metrics

     def _process_esxi_stats(self, stats: Dict[str, Any], timestamp: int) -> list:
        """Process ESXi stats into vROps format"""
        metrics = []
        total_threads_over_threshold = 0
        
        # Process host-level stats
        hosts_stats = stats.get('hosts', {})
        for host_id, host_stats in hosts_stats.items():
            vmnic_stats = host_stats.get('vmnic_stats', {})
            host_threads_over_threshold = 0
            
            # Process all vmnic entries dynamically
            for vmnic, vmnic_data in vmnic_stats.items():
                # Skip the 'ens' key as it's handled separately
                if vmnic == 'ens':
                    continue
                    
                # Add max values per vmnic if they are non-zero
                if vmnic_data.get('max_used', 0) > 0:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|{vmnic}|max_values|used',
                        'timestamps': [timestamp],
                        'data': [vmnic_data['max_used']]
                    })
                if vmnic_data.get('max_ready', 0) > 0:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|{vmnic}|max_values|ready',
                        'timestamps': [timestamp],
                        'data': [vmnic_data['max_ready']]
                    })
                
                # Process thread stats for each vmnic and count high usage threads
                for thread_name, thread_stats in vmnic_data.get('threads', {}).items():
                    if thread_stats.get('used', 0) > 0:
                        thread_usage = thread_stats['used']
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|{vmnic}|{thread_name}|used',
                            'timestamps': [timestamp],
                            'data': [thread_usage]
                        })
                        if thread_usage >= self.usage_threshold:
                            host_threads_over_threshold += 1
                            
                    if thread_stats.get('ready', 0) > 0:
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|{vmnic}|{thread_name}|ready',
                            'timestamps': [timestamp],
                            'data': [thread_stats['ready']]
                        })
            
            # Process EnsNetWorld stats if present
            ens_data = vmnic_stats.get('ens', {})
            if ens_data:
                # Add max values for EnsNetWorld
                if ens_data.get('max_used', 0) > 0:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|max_values|used',
                        'timestamps': [timestamp],
                        'data': [ens_data['max_used']]
                    })
                if ens_data.get('max_ready', 0) > 0:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|max_values|ready',
                        'timestamps': [timestamp],
                        'data': [ens_data['max_ready']]
                    })
                
                # Process TX threads and count high usage
                for thread_name, thread_stats in ens_data.get('tx', {}).get('threads', {}).items():
                    if thread_stats.get('used', 0) > 0:
                        thread_usage = thread_stats['used']
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|TX|{thread_name}|used',
                            'timestamps': [timestamp],
                            'data': [thread_usage]
                        })
                        if thread_usage >= self.usage_threshold:
                            host_threads_over_threshold += 1
                            
                    if thread_stats.get('ready', 0) > 0:
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|TX|{thread_name}|ready',
                            'timestamps': [timestamp],
                            'data': [thread_stats['ready']]
                        })
                
                # Process RX threads and count high usage
                for thread_name, thread_stats in ens_data.get('rx', {}).get('threads', {}).items():
                    if thread_stats.get('used', 0) > 0:
                        thread_usage = thread_stats['used']
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|RX|{thread_name}|used',
                            'timestamps': [timestamp],
                            'data': [thread_usage]
                        })
                        if thread_usage >= self.usage_threshold:
                            host_threads_over_threshold += 1
                            
                    if thread_stats.get('ready', 0) > 0:
                        metrics.append({
                            'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|EnsNetWorld|RX|{thread_name}|ready',
                            'timestamps': [timestamp],
                            'data': [thread_stats['ready']]
                        })
            
            # Add host-level threshold counter
            if host_threads_over_threshold > 0:
                metrics.append({
                    'statKey': f'EdgePerformanceMetrics|ESXi|{host_id}|threads_over_usage_threshold',
                    'timestamps': [timestamp],
                    'data': [host_threads_over_threshold]
                })
                total_threads_over_threshold += host_threads_over_threshold
        
        # Add total threads over threshold across all ESXis
        if total_threads_over_threshold > 0:
            metrics.append({
                'statKey': 'EdgePerformanceMetrics|ESXi|max_values|threads_over_usage_threshold_(All_ESXis)',
                'timestamps': [timestamp],
                'data': [total_threads_over_threshold]
            })

        return metrics
        
     def _merge_stats(self, default_stats: Dict, collected_stats: Dict) -> Dict:
        """
        Merge collected stats with defaults, ensuring dynamic vmnic handling.
        Creates default structure for any new vmnics found in collected stats.
        """
        merged = copy.deepcopy(default_stats)
        
        try:
            # Special handling for vmnic_stats to ensure dynamic vmnic support
            if 'hosts' in collected_stats:
                for host_id, host_stats in collected_stats['hosts'].items():
                    if 'vmnic_stats' in host_stats:
                        # Create host entry if it doesn't exist
                        if host_id not in merged['hosts']:
                            merged['hosts'][host_id] = {'vmnic_stats': {}}
                            
                        # Process each vmnic found in collected stats
                        for vmnic, vmnic_data in host_stats['vmnic_stats'].items():
                            if vmnic not in merged['hosts'][host_id]['vmnic_stats']:
                                # Add default structure for new vmnic
                                if vmnic == 'ens':
                                    merged['hosts'][host_id]['vmnic_stats'][vmnic] = {
                                        'max_used': 0,
                                        'max_ready': 0,
                                        'tx': {'threads': {}},
                                        'rx': {'threads': {}}
                                    }
                                else:
                                    merged['hosts'][host_id]['vmnic_stats'][vmnic] = {
                                        'max_used': 0,
                                        'max_ready': 0,
                                        'threads': {}
                                    }
                            
                            # Merge the data
                            if isinstance(vmnic_data, dict):
                                merged['hosts'][host_id]['vmnic_stats'][vmnic] = self._merge_stats(
                                    merged['hosts'][host_id]['vmnic_stats'][vmnic],
                                    vmnic_data
                                )
                            else:
                                merged['hosts'][host_id]['vmnic_stats'][vmnic] = vmnic_data

            # Handle other keys normally
            for key, value in collected_stats.items():
                if key != 'hosts':  # Skip hosts as we handled it specially above
                    if isinstance(value, dict):
                        if key in merged:
                            merged[key] = self._merge_stats(merged[key], value)
                        else:
                            merged[key] = value
                    else:
                        merged[key] = value
        except Exception as e:
            self.logger.error(f"Error merging stats: {e}")
        
        return merged


     def collect_cluster_metrics(self, edge_stats: Dict[str, Any], esxi_stats: Dict[str, Any], timestamp: int) -> list:
        """Generate cluster-level metrics including average and total values"""
        metrics = []
        
        # Collect Edge node max values (only for CPU stats)
        edge_max_values = {
            'cpu_usage': 0.0,
            'crypto': 0.0,
            'slowpath': 0.0,
            'intercore': 0.0
        }

        # Collect averages and totals
        flow_cache_totals = {
            'micro_hit_rate': {'sum': 0.0, 'count': 0},
            'mega_hit_rate': {'sum': 0.0, 'count': 0}
        }
        
        interface_totals = {
            'rx_misses': 0.0,
            'tx_errors': 0.0
        }

        # Process Edge nodes stats
        for node_stats in edge_stats.get('nodes', {}).values():
            # CPU stats max values
            for core_stats in node_stats.get('performance', {}).get('cpu_stats', {}).values():
                edge_max_values['cpu_usage'] = max(edge_max_values['cpu_usage'], 
                                                core_stats.get('usage', 0) or 0)
                edge_max_values['crypto'] = max(edge_max_values['crypto'], 
                                            core_stats.get('crypto', 0) or 0)
                edge_max_values['slowpath'] = max(edge_max_values['slowpath'], 
                                                core_stats.get('slowpath', 0) or 0)
                edge_max_values['intercore'] = max(edge_max_values['intercore'], 
                                                core_stats.get('intercore', 0) or 0)

            # Flow cache stats for averaging
            flow_stats = node_stats.get('performance', {}).get('flow_cache_stats', {})
            for cache_type in ['micro_hit_rate', 'mega_hit_rate']:
                for hit_rate in flow_stats.get(cache_type, {}).values():
                    if hit_rate is not None and hit_rate > 0:  # Only count non-zero values
                        flow_cache_totals[cache_type]['sum'] += hit_rate
                        flow_cache_totals[cache_type]['count'] += 1

            # Interface stats totals
            for interface_stats in node_stats.get('interfaces', {}).values():
                rx_misses = interface_stats.get('rx_misses', 0)
                tx_errors = interface_stats.get('tx_errors', 0)
                if rx_misses > 0:
                    interface_totals['rx_misses'] += rx_misses
                if tx_errors > 0:
                    interface_totals['tx_errors'] += tx_errors

        # Add Edge node max values metrics if they are non-zero
        for key, value in edge_max_values.items():
            if value > 0:
                metrics.append({
                    'statKey': f'EdgePerformanceMetrics|EdgeNodes|max_values|{key}',
                    'timestamps': [timestamp],
                    'data': [value]
                })

        # Add flow cache averages
        for cache_type, values in flow_cache_totals.items():
            if values['count'] > 0:
                average = values['sum'] / values['count']
                if average > 0:
                    metrics.append({
                        'statKey': f'EdgePerformanceMetrics|EdgeNodes|average_values|{cache_type}',
                        'timestamps': [timestamp],
                        'data': [average]
                    })

        # Add interface totals if they are non-zero
        for stat_name, total in interface_totals.items():
            if total > 0:
                metrics.append({
                    'statKey': f'EdgePerformanceMetrics|EdgeNodes|total_values|{stat_name}',
                    'timestamps': [timestamp],
                    'data': [total]
                })

        # Add ESXi max values to metrics if they are non-zero
        esxi_max_values = esxi_stats.get('max_values', {})
        if esxi_max_values.get('used', 0) > 0:
            metrics.append({
                'statKey': 'EdgePerformanceMetrics|ESXi|max_values|used',
                'timestamps': [timestamp],
                'data': [esxi_max_values['used']]
            })
        if esxi_max_values.get('ready', 0) > 0:
            metrics.append({
                'statKey': 'EdgePerformanceMetrics|ESXi|max_values|ready',
                'timestamps': [timestamp],
                'data': [esxi_max_values['ready']]
            })

        return metrics
    
     def collect_and_publish_stats(self):
        """Collect and publish both Edge and ESXi stats"""
        try:
            # Get vROps resource mappings using updated config
            node_ids = self._get_vrops_resource_map('NSXTAdapter', 'TransportNode')
            cluster_ids = self._get_vrops_resource_map('NSXTAdapter', 'EdgeCluster')
            
            if not node_ids or not cluster_ids:
                self.logger.error("Failed to get resource mappings from vROps")
                return False

            if self.verbose:
                self.logger.info(f"Found {len(node_ids)} edge nodes and {len(cluster_ids)} clusters in vROps")
                self.logger.info(f"Available cluster IDs in vROps: {cluster_ids}")

            current_time = round(time.time() * 1000)
            resource_stats = []

            # Collect Edge Node Stats
            edge_stats = {'nodes': {}}
            try:
                edge_collector = NSXEdgeStatsCollector()
                collected_edge_stats = edge_collector.collect_all_stats()
                if collected_edge_stats and 'nodes' in collected_edge_stats:
                    edge_stats = self._merge_stats(
                        {'nodes': {node_id: copy.deepcopy(DEFAULT_EDGE_NODE_STATS) for node_id in node_ids}},
                        collected_edge_stats
                    )
                else:
                    if self.verbose:
                        self.logger.warning("Edge stats collection returned no data, using defaults")
            except Exception as e:
                self.logger.error(f"Edge stats collection error: {e}")
                # Use defaults for all known nodes
                edge_stats = {'nodes': {node_id: copy.deepcopy(DEFAULT_EDGE_NODE_STATS) for node_id in node_ids}}

            # Process Edge Node stats
            edge_metrics_count = 0
            for node_id, stats in edge_stats['nodes'].items():
                if node_id in node_ids:
                    vrops_id = node_ids[node_id]
                    metrics = self._process_edge_stats(stats, current_time)
                    if metrics:
                        edge_metrics_count += len(metrics)
                        resource_stats.append({
                            'id': vrops_id,
                            'stat-contents': metrics
                        })
                elif self.verbose:
                    self.logger.warning(f"Node {node_id} not found in vROps mappings")
            
            if self.verbose:
                self.logger.info(f"Processed {edge_metrics_count} edge metrics")

            # Collect ESXi stats
            first_cluster = next(iter(self.edge_clusters))
            esxi_stats = copy.deepcopy(DEFAULT_ESXI_STATS)

            if self.verbose:
                self.logger.info(f"Processing cluster: {first_cluster}")
            
            try:
                esxi_collector = ESXiStatsCollector(verbose=self.verbose)
                collected_esxi_stats = esxi_collector.collect_cluster_stats(first_cluster)
                if collected_esxi_stats:
                    esxi_stats = self._merge_stats(esxi_stats, collected_esxi_stats)
            except Exception as e:
                self.logger.error(f"ESXi stats collection error: {e}")
                # Continue with default stats

            # Process ESXi and cluster stats
            if first_cluster in cluster_ids:
                vrops_id = cluster_ids[first_cluster]
                if self.verbose:
                    self.logger.info(f"Found vROps ID for cluster: {vrops_id}")
                
                # Combine all metrics for this cluster
                combined_metrics = []
                
                # Add ESXi metrics
                esxi_metrics = self._process_esxi_stats(esxi_stats, current_time)
                if esxi_metrics:
                    combined_metrics.extend(esxi_metrics)
                
                # Add cluster-level metrics
                cluster_metrics = self.collect_cluster_metrics(edge_stats, esxi_stats, current_time)
                if cluster_metrics:
                    combined_metrics.extend(cluster_metrics)
                    if self.verbose:
                        self.logger.info(f"Added {len(cluster_metrics)} cluster-level metrics")
                
                if combined_metrics:
                    resource_stats.append({
                        'id': vrops_id,
                        'stat-contents': combined_metrics
                    })
                else:
                    self.logger.warning("No ESXi or cluster metrics were processed")
            else:
                self.logger.warning(f"Cluster {first_cluster} not found in vROps mappings")

            # Publish to vROps
            if resource_stats:
                payload = {'resource-stat-content': resource_stats}
                if self.verbose:
                    self.logger.info(f"Publishing payload to vROps: {json.dumps(payload, indent=2)}")
                    self.logger.info(f"Publishing {len(resource_stats)} resources with metrics to vROps")
                return self._publish_to_vrops(payload)
            else:
                self.logger.warning("No stats collected to publish. Check vROps mappings and metric processing.")
                return False

        except Exception as e:
            self.logger.error(f"Error in collect_and_publish_stats: {e}")
            notify.my_function(
                f"Failed to collect and publish stats: {e}",
                self.access_token
            )
            return False

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('edge_monitoring.log', mode='a'),  # 'a' means append
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Suppress insecure request warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    collector = StatsCollector(verbose=False)
    try:
        success = collector.collect_and_publish_stats()
        if not success:
            logging.error("Failed to collect and publish stats")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Failed to collect and publish stats: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()