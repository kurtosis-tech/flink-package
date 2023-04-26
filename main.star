FLINK_IMAGE = "flink:1.17.0-scala_2.12-java11"
DEFAULT_NUMBER_OF_TASK_MANAGERS = 2
NUM_TASK_MANAGERS_ARG_NAME = "num_task_managers"
FLINK_NODE_PREFIX = "task-manager-"

FLINK_WEB_UI_SERVER_PORT_NUMBER = 8081
FLINK_GRPC_SERVER_PORT_NUMBER = 6123
FLINK_BLOB_SERVER_PORT_NUMBER = 6124
FLINK_QUERY_SERVER_PORT_NUMBER = 6125
FLINK_GRPC_SERVER_PORT_AUTOMATIC_WAIT_DISABLE = None
FLINK_QUERY_SERVER_PORT_AUTOMATIC_WAIT_DISABLE = None

FLINK_JOB_MANAGER_HEAP_SIZE = "1024M"
FLINK_JOB_MANAGER_HOSTNAME = "jobmanager"

FLINK_TASK_MANAGER_HEAP_SIZE = "256M"
FLINK_TASK_MANAGER_HOSTNAME = "taskmanager"

FLINK_LIB_JARS_EXTRA_ARG_NAME = "flink-lib-jars-extra"
FLINK_LIB_JARS_EXTRA_PATH = "/opt/flink/lib/extras"

def run(plan, args):
    num_task_managers, flink_lib_jars_extra = process_arguments(plan, args)
    plan.print("Starting Flink cluster with %d task managers" % num_task_managers)

    JOB_MANAGER_PROPERTIES = \
        '''jobmanager.memory.heap.size: %s
jobmanager.rpc.address: %s''' % (FLINK_JOB_MANAGER_HEAP_SIZE, FLINK_JOB_MANAGER_HOSTNAME)
    plan.print(JOB_MANAGER_PROPERTIES)

    job_manager_config = ServiceConfig(
        image=FLINK_IMAGE,
        ports={
            "web-ui": PortSpec(
                number=FLINK_WEB_UI_SERVER_PORT_NUMBER,
                transport_protocol="TCP",
                application_protocol="http",
            ),
            "grpc-server": PortSpec(
                number=FLINK_GRPC_SERVER_PORT_NUMBER,
                transport_protocol="TCP",
                application_protocol="grpc",
                wait = FLINK_GRPC_SERVER_PORT_AUTOMATIC_WAIT_DISABLE,
            ),
            "blob-server": PortSpec(
                number=FLINK_BLOB_SERVER_PORT_NUMBER,
                transport_protocol="TCP",
            ),
            "query-server": PortSpec(
                number=FLINK_QUERY_SERVER_PORT_NUMBER,
                transport_protocol="TCP",
                wait = FLINK_QUERY_SERVER_PORT_AUTOMATIC_WAIT_DISABLE,
            ),
        },
        cmd=[
            FLINK_JOB_MANAGER_HOSTNAME,
        ],
        env_vars={
            "FLINK_PROPERTIES": JOB_MANAGER_PROPERTIES,
        },
        files=flink_lib_jars_extra,
    )
    job_manager_service = plan.add_service(name=FLINK_JOB_MANAGER_HOSTNAME, config=job_manager_config)
    plan.print("Assigned job manager hostname: " + str(job_manager_service.hostname))

    FLINK_TASK_MANAGER_PROPERTIES = \
        '''taskmananger.memory.heap.size: %s
jobmanager.rpc.address: %s''' % (FLINK_TASK_MANAGER_HEAP_SIZE, job_manager_service.hostname)
    plan.print(FLINK_TASK_MANAGER_PROPERTIES)

    for node in range(0, num_task_managers):
        node_name = get_service_name(node)
        task_manager_config = ServiceConfig(
            image=FLINK_IMAGE,
            cmd=[
                FLINK_TASK_MANAGER_HOSTNAME,
            ],
            env_vars={
                "FLINK_PROPERTIES": FLINK_TASK_MANAGER_PROPERTIES,
            },
            files=flink_lib_jars_extra,
        )
        plan.add_service(name=node_name, config=task_manager_config)
    return

def get_service_name(node_idx):
    return FLINK_NODE_PREFIX + str(node_idx)

def process_arguments(plan, args):
    num_task_managers = DEFAULT_NUMBER_OF_TASK_MANAGERS
    if NUM_TASK_MANAGERS_ARG_NAME in args:
        num_task_managers = args[NUM_TASK_MANAGERS_ARG_NAME]
    if num_task_managers == 0:
        fail("Provide at least 1 task manager to run the Flink cluster, got 0")

    flink_lib_jars_extra = {}
    if FLINK_LIB_JARS_EXTRA_ARG_NAME in args:
        flink_lib_jars_extra_artifact_name = args[FLINK_LIB_JARS_EXTRA_ARG_NAME]
        flink_lib_jars_extra = {FLINK_LIB_JARS_EXTRA_PATH: flink_lib_jars_extra_artifact_name}
        plan.print("Extra Flink lib jars will be loaded with artifact name (id): %s to path %s" % (flink_lib_jars_extra_artifact_name, FLINK_LIB_JARS_EXTRA_PATH))

    return num_task_managers, flink_lib_jars_extra
