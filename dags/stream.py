from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

import uuid
import json
import requests
import logging
import time


def get_data():
    # Get data from API
    res = requests.get("https://randomuser.me/api/").json()
    res = res['results'][0]

    return res


def format_data(res):
    data = {}
 
    location = res['location']

    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email'] 
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data




# Define DAG
@dag(
    dag_id='stream_data',
    default_args={
        'owner': 'hiep',
        'start_date': datetime(2023, 12, 30, 10, 00),
    },
    schedule_interval='@daily',
    catchup=False,
    tags=['kafka']
)
def kafka_stream() -> DAG:
    
    @task(task_id='stream_data_from_api')
    def stream_data():
        from kafka import KafkaProducer
        
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], 
                                 max_block_ms=5000)
        
        curr_time = time.time()

        while time.time() < curr_time + 60:
            try:
                res = get_data()
                data = format_data(res)

                producer.send(topic='users_created', 
                              value=json.dumps(data).encode('utf-8'))

            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue


    chain(stream_data())
    