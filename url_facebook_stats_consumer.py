import urllib
import facebook
import boto3
import json
import psycopg2
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor()

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential[0],
        aws_secret_access_key=aws_credential[1],
        region_name=aws_credential[2]
    )
    sqs = aws_session.resource('sqs')
    credentials_queue = sqs.get_queue_by_name(QueueName='facebook-credentials')
    message = credentials_queue.receive_messages()
    credentials = json.loads(message[0].body)
    message[0].delete()

    graph = facebook.GraphAPI(access_token=credentials['app_id'] + '|' + credentials['app_secret'],
                              version='3.0')

    queue_empty = False
    queue = sqs.get_queue_by_name(QueueName='facebook-urls')
    while not queue_empty:
        message = queue.receive_messages()
        if len(message) == 0:
            queue_empty = True
        else:
            received_message = json.loads(message[0].body)
            message[0].delete()
            for object in received_message['urls']:
                error = None
                stats = None
                og_object = None
                try:
                    stats = graph.get_object(
                        urllib.parse.quote_plus(object['url']),
                        fields='og_object,engagement')
                    if stats.get('og_object',{}).get('id') is not None:
                        og_object = graph.get_object(stats.get('og_object',{}).get('id'))
                except Exception as e:
                    error = repr(e)

                if stats is not None:
                    stats = json.dumps(stats)
                if og_object is not None:
                    og_object = json.dumps(og_object)

                cur.execute("""insert into facebook_url_stats
                                (query_alias, url, created_at, google, twitter,
                                error, response, og_object)
                                values 
                                (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (received_message['query_alias'],
                             object['url'],
                             object['created_at'],
                             True if object['google'] == 1 else False,
                             True if object['twitter'] == 1 else False,
                             error,
                             stats,
                             og_object))
                conn.commit()

    conn.close()
