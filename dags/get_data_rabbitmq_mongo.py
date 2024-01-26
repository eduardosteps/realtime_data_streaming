from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import json
import pika
import requests

default_args = {
    'owner': 'Eduardo Passos',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'get_data_rabbitmq_mongo',
    default_args=default_args,
    description='A DAG to fetch random user data',
    schedule=timedelta(seconds=30),
)

def get_data_from_api(**kwargs):
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['results'][0]
        kwargs['ti'].xcom_push(key='api_data', value=data)
    else:
        raise ValueError("Failed to fetch data from API")
    
get_data_from_api_task = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag,
)

def transform_data(**kwargs):
    ti = kwargs['ti']
    api_data = ti.xcom_pull(key='api_data', task_ids='get_data_from_api')

    location = api_data['location']
    transformed_data = {
        'first_name': api_data['name']['first'],
        'last_name': api_data['name']['last'],
        'gender': api_data['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'lat': location['coordinates']['latitude'],
        'long': location['coordinates']['longitude'],
        'email': api_data['email'],
        'username': api_data['login']['username'],
        'dob': api_data['dob']['date'],
        'registered_date': api_data['registered']['date'],
        'phone': api_data['phone'],
        'picture': api_data['picture']['medium']
    }
    ti.xcom_push(key='transformed_data', value=transformed_data)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

def send_to_rabbitmq(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    rabbitmq_server = 'rabbitmq'
    username = 'guest'
    password = 'guest'
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server, credentials=credentials))
    channel = connection.channel()
    queue_name = 'Dictionary_from_Randomuser_API'
    channel.queue_declare(queue=queue_name)
    message = json.dumps(data).encode('utf-8')
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f" [x] Sent '{message}'")
    connection.close()

send_to_rabbitmq_task = PythonOperator(
    task_id='send_to_rabbitmq',
    python_callable=send_to_rabbitmq,
    dag=dag,
)

def send_to_mongodb(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='send_to_rabbitmq')

    username = "root"
    password = "example"
    host = "localhost"
    port = "27017"

    client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")

    db = client['randomuser_db']
    collection = db['randomuser']

    if data:
        collection.insert_one(data)
        print("Data inserted into MongoDB successfully.")
    else:
        print("No data received from previous task.")

send_to_mongodb_task = PythonOperator(
    task_id='send_to_mongodb',
    python_callable=send_to_mongodb,
    dag=dag,
)

get_data_from_api_task >> transform_data_task >> send_to_rabbitmq_task >> send_to_mongodb_task
