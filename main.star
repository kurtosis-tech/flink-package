FLINK_IMAGE = "flink:latest"
DEFAULT_NUMBER_OF_NODES = 5
NUM_NODES_ARG_NAME = "num_nodes"
FLINK_NODE_PREFIX="flink-node-"

def run(plan, args):

    num_nodes = DEFAULT_NUMBER_OF_NODES
    if hasattr(args, NUM_NODES_ARG_NAME):
        num_nodes = args.num_nodes

    if num_nodes == 0:
        fail("Need at least 1 node to start Flink cluster, got 0")

    job_manager_config = ServiceConfig(
        image = FLINK_IMAGE,
        ports = {
            "web-ui": PortSpec(
                number = 8081,
                transport_protocol = "TCP",
                application_protocol = "web-ui",
            ),
            "grpc-server": PortSpec(
                number = 6123,
                transport_protocol = "TCP",
                application_protocol = "grpc",
            ),
            "blob-server": PortSpec(
                number = 6124,
                transport_protocol = "TCP",
                application_protocol = "blob",
            ),
            "query-server": PortSpec(
                number = 6125,
                transport_protocol = "TCP",
                application_protocol = "query",
            ),
        },
        cmd = [
            "jobmanager",
        ],
    )
    job_manager_service = plan.add_service(service_name = "jobmanager", config = job_manager_config)
    plan.print("job manager hostname: "+str(job_manager_service.hostname))

    FLINK_TASKMANAGER_PROPERTIES="jobmanager.rpc.address: "+str(job_manager_service.hostname)
    for node in range(0, num_nodes):
        node_name = get_service_name(node)
        task_manager_config = ServiceConfig(
            image = FLINK_IMAGE,
            cmd = [
                "taskmanager",
            ],
            env_vars = {
                "FLINK_PROPERTIES": FLINK_TASKMANAGER_PROPERTIES,
            }
        )
        plan.add_service(service_name = node_name, config = task_manager_config)
    return

def get_service_name(node_idx):
    return FLINK_NODE_PREFIX + str(node_idx)
