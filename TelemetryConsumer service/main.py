from fastapi import FastAPI
from schema import TelemetryIngestionBody
import logging
import aio_pika
from contextlib import asynccontextmanager


logging.basicConfig(level=logging.DEBUG)

@asynccontextmanager
async def lifespan():
    global mq_connection,mq_channel

    try:
        mq_connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq:5672/")
        mq_channel = await mq_connection.channel()

        await mq_channel.declare_queue("telemetry_queue", durable=True)

    except Exception as e:

         logging.error(f"Failed to connect to RabbitMQ: {e}")

    yield


    if mq_connection:
        await mq_connection.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def healthCheck():
    return "TelemetryConsumer service is running"


@app.post("/ingest")
async def ingestTelemetry(body:TelemetryIngestionBody):
    user_id = body.user_id
    model_name = body.model_name

    response = f"{user_id} using {model_name}"
    
    return response

