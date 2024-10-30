from pydantic import BaseModel


class CollectJobRequest(BaseModel):
    """Describe body for collect data API endpoint."""
    date: str
    raw_dir: str


class TransformJobRequest(BaseModel):
    """Describe body for transfrom API endpoint."""
    raw_dir: str
    stg_dir: str