from pydantic import BaseModel

class TelemetryIngestionBody(BaseModel):
    user_id: str
    model_name: str
    input_tokens: int
    output_tokens: int