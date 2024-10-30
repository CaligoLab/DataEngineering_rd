from fastapi import HTTPException, APIRouter
from starlette import status

from apis.collect_api.service import save_raw_data
from apis.schemas.models import CollectJobRequest

collect_router = APIRouter()


@collect_router.post("/", status_code=status.HTTP_201_CREATED)
def save_data(body: CollectJobRequest):
    """Save data to dedicated directory based on request body."""
    try:
        save_raw_data(raw_dir=body.raw_dir, date=body.date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
