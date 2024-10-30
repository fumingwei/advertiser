from pydantic import BaseModel, Field


class OeOpenAccountSchema(BaseModel):
    customer_id: str = ""
    oe_number: str = Field(..., min_length=1, max_length=32)
