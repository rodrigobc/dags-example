# -*- coding: utf-8 -*-
"""
Created on Fri Jul  6 16:44:35 2018

@author: rodri
"""
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

yesterday = datetime.combine(datetime.today() - timedelta(days=1),
                             datetime.min.time())

hostname = Variable.get('sshhost')
port = 22
username = Variable.get('sshusername')
password = Variable.get('sshpassword')

filename = '/home/ubuntu/dev/dummy_file.txt'
remotefile = '/bi/bi/teste.log'


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
client = createSSHClient(hostname,port,username,password)
 


with DAG('v1_2_transformfile', schedule_interval='*/5 * * * *',
         default_args=default_args) as dag:
    def should_run(ds, **kwargs):

        if datetime.now() < kwargs['execution_date'] + timedelta(minutes=120):
            return "s3_chk"
        else:
            return "no_run"


    start = BranchPythonOperator(
        task_id='start',
        provide_context=True,
        python_callable=should_run,
    )
    
    run_this = PythonOperator(
        task_id='print_the_context',
        provide_context=True,
        python_callable=print_context,
        dag=dag)


    task = PythonOperator(
        task_id='send_file_remote',
        #trigger_rule='all_success',
        python_callable=sendFile,
        op_kwargs={'client': client, 'filename': filename, 'remotefile': remotefile },
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
    
    s3_transform = S3FileTransformOperator(
        task_id='s3_transform',
        depends_on_past=False,
	#trigger_rule='all_success',
        source_s3_key="{}input/{}".format(Variable.get('s3_buckey'),Variable.get('s3_filename')),
        dest_s3_key="{}output/{}{}".format(Variable.get('s3_buckey'),datetime.today().strftime('%Y%m%d%H%M'),Variable.get('s3_filename')),
        transform_script='/home/ubuntu/airflow/dag_scripts/transform.sh',
        source_s3_conn_id='dev1_s3',
        dest_s3_conn_id='dev1_s3',
        replace=True,
        dag=dag)	
    
    s3_remove = BashOperator(
	task_id='s3_remove',
	bash_command='aws s3 rm {}input/{}'.format(Variable.get('s3_buckey'),Variable.get('s3_filename')),
	trigger_rule='all_success',
        dag=dag)

    no_run = DummyOperator(task_id='no_run')

    end = DummyOperator(
        trigger_rule='all_done',
        task_id='end')


    #run_this >> start >> s3_chk >> s3_transform >> s3_remove >> task >> end
    run_this >> start >> s3_chk >> s3_transform >> s3_remove >> end

    start >> no_run >> end

