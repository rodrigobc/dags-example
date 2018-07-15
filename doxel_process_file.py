# -*- coding: utf-8 -*-
"""
Created on Fri Jul  15 16:44:35 2018

@author: Rodrigo Bandeira <rodrigodeimbassai@gmail.com
"""
from airflow.hooks import S3_hook
import json

from pprint import pprint
import paramiko
import paramiko.sftp_client
from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator,PythonOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

from airflow.operators.email_operator import EmailOperator

yesterday = datetime.combine(datetime.today() - timedelta(days=1),
                             datetime.min.time())



### Variables
s3conn = 's3-doxel-airflow'
prefix_key='new-project/input/' 
bucket='doxel-airflow'
workspaceId='ws-510f77jx6'
profile='doxel-airflow'
emails='rodrigodeimbassai@gmail.com'
#hostname = Variable.get('sshhost')
#port = 22
#username = Variable.get('sshusername')
#password = Variable.get('sshpassword')
filename = '/home/ubuntu/dev/dummy_file.txt'
#remotefile = '/bi/bi/teste.log'


default_args = {
    'owner': 'rodrigo',
    'depends_on_past': False,
    'start_date': yesterday,
    'email': ['rodrigodeimbassai@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}




def createSSHClient(server,port,user,password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client

def create_structure(projectID):
    #create structure of folder, send a dummy file for each folder
    return 0

def sendFile(client,filename,remotefile):
    sftp = client.open_sftp()
    sftp.put(filename,remotefile)
    sftp.close()
    client.close()
    return 0

 
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

#paramiko.util.log_to_file('paramiko.log')
#client = createSSHClient(hostname,port,username,password)
 


with DAG('v0_1_process_file', schedule_interval='*/30 * * * *',
         default_args=default_args) as dag:

    def should_run(ds, **kwargs):
        if datetime.now() < kwargs['execution_date'] + timedelta(minutes=120):
            return "s3_chk"
        else:
            return "no_run"
    
    def workspace_ready(ds,**kwargs):
	# execute aws workspaces describe-workspaces --profile rodrigo_doxel | grep  "\"State\""  checking status possible values
	# STOPPED - STARTING - AVAILABLE 
	status='AVAILABLE'
	if status == 'AVAILABLE':
	    return "send_file"
        else:
            return "chk_workspace"
    

    start = BranchPythonOperator(
        task_id='start',
        provide_context=True,
        python_callable=should_run,
    )
    
    chk_workspace = BranchPythonOperator(
        task_id='chk_workspace',
        provide_context=True,
        python_callable=workspace_ready,
    )
    

    run_this = PythonOperator(
        task_id='print_the_context',
        provide_context=True,
        python_callable=print_context,
        dag=dag)


    send_file = PythonOperator(
        task_id='send_file',
        #trigger_rule='all_success',
        python_callable=sendFile,
        op_kwargs={'filename': filename },
        dag=dag)

    s3_chk = S3KeySensor(
        task_id='s3_chk',
        s3_conn_id='dev1_s3',
        depends_on_past=False,
        poke_interval=2,
        timeout=15,
        soft_fail=False,
        bucket_key='{}input/{}'.format(Variable.get('s3_buckey'),Variable.get('s3_filename')),
        bucket_name=None,
        wildcard_match=False,
        dag=dag)
    
    s3_create_project = PythonOperator(
        task_id='s3_create_project',
        depends_on_past=False,
	op_kwargs={'project_id': '', 'bucket': filename },
        python_callable=create_structure,
        dag=dag)	
    
    turn_on_workspace = BashOperator(
	task_id='turn_on_workspace',
	bash_command='aws workspaces start-workspaces --start-workspace-requests WorkspaceId={} --profile {}'.format(workspaceId,profile),
	trigger_rule='all_success',
        dag=dag)

    execute_command = DummyOperator(
	task_id='execute_command',
	dag=dag)

    move_outputfiles = DummyOperator(
	task_id='move_outputfiles',
	dag=dag)
    
    turn_off_workspace = BashOperator(
	task_id='turn_off_workspace',
	bash_command='aws workspaces stop-workspaces --stop-workspace-requests WorkspaceId={} --profile {}'.format(workspaceId,profile),
	trigger_rule='all_success',
        dag=dag)

    email_task = EmailOperator(
        to=emails,
        task_id='email_task',
        subject='Templated Subject: start_date {{ ds }}',
        params={'content1': 'random'},
        html_content="Templated Content: content1 - {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
        dag=dag)
 
    no_run = DummyOperator(task_id='no_run')

    end = DummyOperator(
        trigger_rule='all_done',
        task_id='end')


    #run_this >> start >> s3_chk >> s3_transform >> s3_remove >> task >> end
    run_this >> start >> s3_chk >> s3_create_project >> end
    s3_chk >> turn_on_workspace >> chk_workspace >> send_file >>execute_command >> move_outputfiles >> turn_off_workspace >> email_task >> end
    email_task << move_outputfiles 
    move_outputfiles >> end
    start >> no_run >> end

