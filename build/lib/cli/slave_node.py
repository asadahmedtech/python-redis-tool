class SlaveNode(object):
    master_id = ""
    ip = ""
    port = 0
    node_id = ""

    def __init__(self, master_id, ip, port, node_id):
        self.master_id = master_id
        self.ip = ip
        if not isinstance(port, int):
            raise TypeError("port must be set to an integer")
        self.port = port
        if not isinstance(node_id, str):
            raise TypeError("node_id must be set to a string")
        self.node_id = node_id

    def __str__(self):
        return 'Node: master_id:%s, node_address:%s:%s, id:%s' % (
            self.master_id, self.ip, self.port, self.node_id)
