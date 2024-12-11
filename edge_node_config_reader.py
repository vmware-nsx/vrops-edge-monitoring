import yaml

def load_edge_node_config(config_file='edge_node_config.yaml'):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    edgeNodeIpMap = config['edge_nodes']
    edgeClusterNodeIDMap = config['edge_clusters']    # Don't transform the structure
    
    return edgeNodeIpMap, edgeClusterNodeIDMap
