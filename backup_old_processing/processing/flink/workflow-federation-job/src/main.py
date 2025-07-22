from pyflink.datastream import StreamExecutionEnvironment

def workflow_federation_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.execute('Workflow Federation') 