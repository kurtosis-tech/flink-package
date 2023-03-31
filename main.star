FLINK_IMAGE = "flink:latest"
DEFAULT_NUMBER_OF_TASK_MANAGERS = 2
NUM_TASK_MANAGERS_ARG_NAME = "num_task_managers"
FLINK_NODE_PREFIX="flink-task-manager-"

FLINK_WEB_UI_SERVER_PORT_NUMBER=8081
FLINK_GRPC_SERVER_PORT_NUMBER=6123
FLINK_BLOB_SERVER_PORT_NUMBER=6124
FLINK_QUERY_SERVER_PORT_NUMBER=6125

FLINK_JOB_MANAGER_HEAP_SIZE="1024M"
FLINK_JOB_MANAGER_HOSTNAME="jobmanager"

FLINK_TASK_MANAGER_HEAP_SIZE="256M"
FLINK_TASK_MANAGER_HOSTNAME="taskmanager"

def run(plan, args):
    num_task_managers = DEFAULT_NUMBER_OF_TASK_MANAGERS
    if NUM_TASK_MANAGERS_ARG_NAME in args:
        num_task_managers = args["num_task_managers"]
    if num_task_managers == 0:
        fail("Provide at least 1 task manager to run the Flink cluster, got 0")

    plan.print("""Starting Flink cluster with %d task managers""" % num_task_managers)

    JOB_MANAGER_PROPERTIES = \
        '''jobmanager.memory.heap.size: %s
jobmanager.rpc.address: %s''' % (FLINK_JOB_MANAGER_HEAP_SIZE, FLINK_JOB_MANAGER_HOSTNAME)
    plan.print(JOB_MANAGER_PROPERTIES)

    job_manager_config = ServiceConfig(
        image = FLINK_IMAGE,
        ports = {
            "web-ui": PortSpec(
                number = FLINK_WEB_UI_SERVER_PORT_NUMBER,
                transport_protocol = "TCP",
                application_protocol = "http",
            ),
            "grpc-server": PortSpec(
                number = FLINK_GRPC_SERVER_PORT_NUMBER,
                transport_protocol = "TCP",
                application_protocol = "grpc",
            ),
            "blob-server": PortSpec(
                number = FLINK_BLOB_SERVER_PORT_NUMBER,
                transport_protocol = "TCP",
            ),
            "query-server": PortSpec(
                number = FLINK_QUERY_SERVER_PORT_NUMBER,
                transport_protocol = "TCP",
            ),
        },
        cmd = [
            FLINK_JOB_MANAGER_HOSTNAME,
        ],
        env_vars = {
            "FLINK_PROPERTIES": JOB_MANAGER_PROPERTIES,
        }
    )
    job_manager_service = plan.add_service(name = FLINK_JOB_MANAGER_HOSTNAME, config = job_manager_config)
    plan.print("Assigned job manager hostname: "+str(job_manager_service.hostname))

    FLINK_TASK_MANAGER_PROPERTIES = \
        '''taskmananger.memory.heap.size: %s
jobmanager.rpc.address: %s''' % (FLINK_TASK_MANAGER_HEAP_SIZE, job_manager_service.hostname)
    plan.print(FLINK_TASK_MANAGER_PROPERTIES)

    for node in range(0, num_task_managers):
        node_name = get_service_name(node)
        task_manager_config = ServiceConfig(
            image = FLINK_IMAGE,
            cmd = [
                FLINK_TASK_MANAGER_HOSTNAME,
            ],
            env_vars = {
                "FLINK_PROPERTIES": FLINK_TASK_MANAGER_PROPERTIES,
            }
        )
        plan.add_service(name = node_name, config = task_manager_config)
    return

def get_service_name(node_idx):
    return FLINK_NODE_PREFIX + str(node_idx)
