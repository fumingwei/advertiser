import uvicorn
from starlette.requests import Request
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from apps import create_app
from tools.constant import RET, error_map
from tools.resp import MyResponse
from tools.exceptions import GatheroneError
from settings.log import web_log

app = create_app()


@app.exception_handler(HTTPException)
async def invalid_token_exception_handler(request: Request, exc: HTTPException):
    web_log.log_error(exc.__str__())
    return MyResponse(code=RET.REQ_ERR, msg=error_map[RET.REQ_ERR], err=exc)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    web_log.log_error(exc.__str__())
    return MyResponse(code=RET.PARAM_ERR, msg=error_map[RET.PARAM_ERR])


@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    web_log.log_error(exc.__str__())
    return MyResponse(code=RET.INVALID_DATA, msg=error_map[RET.INVALID_DATA], err=exc)


@app.exception_handler(GatheroneError)
async def gatherone_exception_handler(request: Request, exc: GatheroneError):
    web_log.log_error(exc.__str__())
    return MyResponse(code=RET.INTERNAL_NETWORK_ERR, msg=str(exc), err=exc)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    web_log.log_error(exc.__str__())
    return MyResponse(code=RET.SERVER_ERR, msg=error_map[RET.SERVER_ERR], err=exc)


if __name__ == "__main__":
    uvicorn.run(
        app="main:app",
        host="0.0.0.0",
        port=8010,
        log_config='./uvicorn_config.json',
        env_file=".env",
        reload=False
    )
