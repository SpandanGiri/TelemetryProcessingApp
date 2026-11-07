from fastapi import FastAPI
from schema import TelemetryIngestionBody
import logging
import aio_pika
from contextlib import asynccontextmanager


logging.basicConfig(level=logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
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

    global mq_channel
    
    payload_json = body.model_dump_json()

    if mq_channel:
        await mq_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=payload_json.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key="telemetry_queue"
                )
        logging.debug(f"Published to RabbitMQ: {body.user_id} using {body.model_name}")
    else:
            logging.error("Could not publish: RabbitMQ channel is not available")

    return {"status": "accepted", "user": body.user_id, "model": body.model_name}


