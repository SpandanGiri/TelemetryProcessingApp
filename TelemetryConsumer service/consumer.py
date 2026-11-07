import asyncio
import json
import logging
import aio_pika
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)

db_session = None
db_insert_stmt = None

async def process_message(message: aio_pika.abc.AbstractIncomingMessage):
    global db_session, db_insert_stmt
    
    async with message.process(): 
        payload = json.loads(message.body.decode())
        

        await asyncio.to_thread(
            db_session.execute, 
            db_insert_stmt, 
            (payload['user_id'], payload['model_name'], payload['input_tokens'], payload['output_tokens'])
        )
        
        logging.info(f"Saved to DB -> User: {payload['user_id']} | Model: {payload['model_name']}")


def setup_cassandra():

    global db_session, db_insert_stmt

    cluster = Cluster(["127.0.0.1"],port=9042)
    db_session = cluster.connect()

    db_session.execute("""
            CREATE KEYSPACE IF NOT EXISTS telemetry_keyspace 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    db_session.set_keyspace('telemetry_keyspace')

    db_session.execute("""
            CREATE TABLE IF NOT EXISTS llm_telemetry (
                user_id text,
                model_name text,
                input_tokens int,
                output_tokens int,
                created_at timestamp,
                PRIMARY KEY (user_id, created_at)
            )
        """)

    db_insert_stmt = db_session.prepare("""
            INSERT INTO llm_telemetry (user_id, model_name, input_tokens, output_tokens, created_at)
            VALUES (?, ?, ?, ?, toTimestamp(now()))
        """)
    
    return db_session, db_insert_stmt


async def main():
    setup_cassandra()

    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1:5672/")
    channel = await connection.channel()
    queue = await channel.declare_queue("telemetry_queue", durable=True)
    
    logging.info("Consumer running! Waiting for messages...")
    
    # Pass the globally-defined function to the consumer
    await queue.consume(process_message)
    
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())