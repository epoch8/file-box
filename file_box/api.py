from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger

from file_box.service import FileBoxServiceProtocol, ItemDTO, ResponseDTO, get_file_box_service

app = FastAPI()


@app.get("/healthz", response_model=int, status_code=status.HTTP_200_OK, tags=["healthz"])
def healthz() -> int:
    return status.HTTP_200_OK

@app.post(
    "/api/v1/upload-file",
    response_model=ResponseDTO,
    status_code=status.HTTP_200_OK,
    tags=["file"]
)
def upload_file(
    item: ItemDTO,
    service: FileBoxServiceProtocol = Depends(get_file_box_service)
) -> ResponseDTO:
    res = service.upload_file(item)
    return res
    
@app.get(
    "/api/v1/file-response/{file_id}",
    response_model=ResponseDTO,
    status_code=status.HTTP_200_OK,
    tags=["file"]
)
def get_file_response(
    file_id: str,
    service: FileBoxServiceProtocol = Depends(get_file_box_service)
) -> ResponseDTO:
    res = service.get_file_response(file_id)
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    return res


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request validation error: {exc_str}")
    content = {"message": "unprocessable entity", "detail": exc_str}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
