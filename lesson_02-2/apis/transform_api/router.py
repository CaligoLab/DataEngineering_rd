from fastapi import HTTPException, APIRouter
from starlette import status

from apis.schemas.models import TransformJobRequest
from apis.transform_api.service import convert_json_to_avro, convert_json_to_csv

transform_router = APIRouter()


@transform_router.post("/", status_code=status.HTTP_201_CREATED)
def transform_data(body: TransformJobRequest):
    """Transform required catalog to avro."""
    try:
        convert_json_to_avro(json_directory=body.raw_dir, avro_directory=body.stg_dir)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@transform_router.post("/csv", status_code=status.HTTP_201_CREATED)
def transform_to_csv(body: TransformJobRequest):
    """Transform required catalog to CSV"""
    try:
        convert_json_to_csv(json_directory=body.raw_dir, csv_directory=body.stg_dir)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
