import logging
import time
import uuid
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware, Request, Response

from clinvar_ingest.api.constants import MS_PER_S

logger = logging.getLogger("api")


class LogRequests(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = uuid.uuid4()
        start_ms = int(time.time())
        logger.info(
            f"{request.method} {request.url.path} id={request_id} start_ms={start_ms}"
        )
        response = await call_next(request)
        elapsed_ms = int((time.time() - start_ms) * MS_PER_S)
        logger.info(
            f"{request.method} {request.url.path} id={request_id} elapsed_ms={elapsed_ms} status_code={response.status_code}"
        )
        return response
