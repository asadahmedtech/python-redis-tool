import logging

from cli.util import is_ip
from cli.util import split_address
from cli.util import is_valid_redis_node
from cli.util import run_redis_cli_cmd
from cli.reshard import extract_cluster_masters

def remove_node_from_cluster(source, target, target_role):
    host, port = split_address(source)

    masterDetails, slaveDetails = get_node_details(host, port)
    master_details_map, slave_details_map = dict(), dict()
    masters_from_slots, master_to_slots = list(), list()

    for master_node in masterDetails:
        master_details_map[master_node.ip] = master_node
        slave_details_map[master_node.node_id] = []

        full_node_address = master_node.ip + ":" + str(master_node.port)
        if full_node_address in target:
            masters_from_slots.append(master_node)
        else:
            masters_to_slots.append(master_node)

    for slave_node in slaveDetails:
        slave_details_map[slave_node.master_id].append(slave_node)
    
    target_node_id_list = list()

    if target_role == "master":
        perform_empty_master_node(masters_to_slots, masters_from_slots, source)
        #Add slaves of target master into target delete list.
        
        for master_node in masters_from_slots:
            for slave_node in slave_details_map[master_node.node_id]:
                target_node_id_list.append(slave_node.node_id)
            target_node_id_list.append(master_node.node_id)
    else:
        for redis_node_address in target:
            host, port = split_address(redis_node_address)
            target_node_id = master_details_map[host].node_id
            target_slave_node_id = slave_details_map[target_node_id][0].node_id
            target_node_id_list.append(target_slave_node_id)

    for redis_node_id in target_node_id_list:
        
        logging.info('Removing %s with the %s role to the cluster ', redis_node_id, target_role)
            
        cmd = ['--cluster', 'del-node', source, redis_node_id]
        result = run_redis_cli_cmd(cmd, True)
        if result.returncode == 0:
            logging.info('[V] Node %s was added to the cluster with role %s', full_node_address, target_role)
        else:
            logging.error(
                '[X] Node %s was NOT added to the cluster. '
                'Check if the given node is in clustered mode, '
                'if is it empty or if this node is already part of the cluster',
                redis_node_id)

def get_node_details(host, port):
    cmd_args = ['-c', '-h', host, '-p', port, 'cluster', 'nodes']
    result = run_redis_cli_cmd(cmd_args, True)
    result_as_array = parse_cmd_output_to_array(result.stdout)
    master_nodes_without_slots, master_nodes_with_slots = extract_cluster_masters(result_as_array)
    return master_nodes_without_slots + master_nodes_with_slots, extract_cluster_slaves(result_as_array)


def parse_cmd_output_to_array(stdout):
    parsed_cmd_result = stdout.decode("utf-8")
    parsed_cmd_result.replace('"', '')
    parsed_cmd_result.replace("'", "")
    parsed_cmd_result.rstrip()
    return re.compile("\n").split(parsed_cmd_result)

def extract_cluster_slaves(array_of_all_nodes):
    slave_nodes = []
    i = 0
    while i < len(array_of_all_nodes):
        node = array_of_all_nodes[i]
        if not ('master' in node or 'noaddr' in node):
            node_as_array = re.compile(' ').split(node)
            if 9 >= len(node_as_array) > 1:
                slave_node_to_add = process_array_with_slave_node_fields(node_as_array)
                if slave_node_to_add is not None:
                    slave_nodes.append(slave_node_to_add)
        i += 1
    return slave_nodes

def process_array_with_slave_node_fields(node_properties_as_array):
    host, port = split_address(node_properties_as_array[1])
    if is_ip(host):
        slave_node = SlaveNode(node_properties_as_array[3], host,
                                 int(port), node_properties_as_array[0])
        logger.debug(slave_node)
        return slave_node
    return None

def perform_empty_master_node(masters_to_slots, masters_from_slots, source):
    amount_of_masters = len(masters_to_slots)
    i = 0
    for master_from_slots in masters_from_slots:
        shards_amount_master_will_give = master_from_slots.calculate_amount_of_shards(amount_of_masters)

        logger.debug("%s will give %s shards per split" % (master_from_slots, shards_amount_master_will_give))

        shards_amount_per_one_master = int(shards_amount_master_will_give)

        for master_to_slots in masters_to_slots:
            cmd_args = ['--cluster', 'reshard', source, '--cluster-from', master_from_slots.node_id,
                        '--cluster-to', master_to_slots.node_id, '--cluster-slots',
                        str(shards_amount_per_one_master),
                        '--cluster-yes']
            logger.debug("Sharding %s to %s %s slots" % (
                master_from_slots.node_id, master_to_slots.node_id, shards_amount_per_one_master))
            run_redis_cli_cmd(cmd_args, False)
            logger.debug('Soon will run sanity check')
            time.sleep(5)
            cmd_args = ['--cluster', 'fix', master_to_slots.ip + ":" + str(master_to_slots.port)]
            result = run_redis_cli_cmd(cmd_args, True)
            logger.debug('Sanity check returned code %s' % (str(result.returncode)))
        i += 1