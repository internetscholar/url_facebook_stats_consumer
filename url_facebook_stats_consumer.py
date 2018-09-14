from urllib import parse
import facebook
import json
import psycopg2
import configparser
import boto3
from psycopg2 import extras
import os
import logging
import traceback
import requests
import time


def main():
    if 'DISPLAY' not in os.environ:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to the database')
    ip = requests.get('http://checkip.amazonaws.com').text.rstrip()
    logging.info('IP: %s', ip)
    url = None

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info("Connected to AWS in %s", aws_credential['region_name'])

    try:
        facebook_credentials_queue = sqs.get_queue_by_name(QueueName='facebook_credentials')
        message = facebook_credentials_queue.receive_messages()
        facebook_credentials = json.loads(message[0].body)
        message[0].delete()
        logging.info("Obtained Facebook credential for %s", facebook_credentials['type'])

        current_credential = 0
        number_of_credentials = len(facebook_credentials['credentials'])
        logging.info('Retrieved %d facebook credentials', number_of_credentials)
        renew_graph_object = True
        graph = None

        url_stats_queue = sqs.get_queue_by_name(QueueName='url_facebook_stats')
        url_stats_empty = False
        request_id = None
        logging.info('Connected to queue twitter_hydration')
        while not url_stats_empty:
            message = url_stats_queue.receive_messages()
            if len(message) == 0:
                url_stats_empty = True
                logging.info('No more messages in queue url_facebook_stats')
            else:
                received_message = json.loads(message[0].body)
                message[0].delete()
                for url in received_message:
                    try_again = True
                    stats = None
                    while try_again:
                        if renew_graph_object:
                            graph = facebook.GraphAPI(
                                access_token=facebook_credentials['credentials'][current_credential]['access_token'],
                                version='3.0')
                            renew_graph_object = False
                            cur.execute("insert into url_facebook_stats_request (credential_id, credential_type, ip) "
                                        "values (%s,%s,%s) returning id",
                                        (facebook_credentials['credentials'][current_credential]['id'],
                                         facebook_credentials['type'],
                                         ip,))
                            request_id = cur.fetchone()['id']
                            conn.commit()
                            logging.info('Current credential: %s - %s',
                                         facebook_credentials['credentials'][current_credential]['id'],
                                         facebook_credentials['type'])
                        try:
                            logging.info('Obtaining stats for %s', url['url'])
                            stats = graph.get_object(parse.quote_plus(url['url']),
                                                     fields='og_object,engagement')
                            try_again = False
                        except facebook.GraphAPIError as e:
                            # error 4 (Application request limit reached) or 17 (User request limit reached)
                            if e.code in (4, 17):
                                logging.info(e.message)
                                renew_graph_object = True
                                current_credential = current_credential + 1
                                if current_credential == number_of_credentials:
                                    logging.info('Used all credentials. Going to sleep for 60 seconds')
                                    current_credential = 0
                                    time.sleep(3660)
                            else:
                                raise e
                    cur.execute("""
                        insert into url_facebook_stats
                            (url, project_name, stats, request_id)
                        values 
                            (%s, %s, %s, %s)
                        """,
                                (url['url'],
                                 url['project_name'],
                                 None if stats is None else json.dumps(stats),
                                 request_id))
                    conn.commit()
                    logging.info('Added stats for %s - %s', url['project_name'], url['url'])
    except Exception:
        conn.rollback()
        logging.info('Exception!')
        cur.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                    (json.dumps(url),
                     traceback.format_exc(),
                     'url_facebook_stats',
                     ip), )
        conn.commit()
        logging.info('Saved record with error information')
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
