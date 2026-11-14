import hmac
import logging
import os
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any
from urllib.parse import quote
from uuid import uuid4

import httpx
import jwt
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg import OperationalError
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

DATABASE_DSN = os.getenv(
    "ANALYTICS_DATABASE_DSN",
    "postgresql://airflow:airflow@postgres:5432/airflow",
)
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://webserver:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "")
OPERATIONS_API_KEY = os.getenv("OPERATIONS_API_KEY", "")
PIPELINE_DAG_ID = "real_estate_analytics"
FRESHNESS_THRESHOLD_HOURS = int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "24"))
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "10"))
DATA_QUALITY_MAX_NULL_RATE = float(os.getenv("DATA_QUALITY_MAX_NULL_RATE", "0.25"))
DATA_QUALITY_MIN_PRICE = Decimal(os.getenv("DATA_QUALITY_MIN_PRICE", "0"))
DATA_QUALITY_MAX_PRICE = Decimal(os.getenv("DATA_QUALITY_MAX_PRICE", "10000"))
VALID_LOCAL_ROLES = {"viewer", "operator", "admin"}
DATA_QUALITY_MISSING_EXPRESSIONS = {
    "listing_id": "listing_id IS NULL",
    "name": "name IS NULL OR BTRIM(name) = ''",
    "host_id": "host_id IS NULL",
    "host_name": "host_name IS NULL OR BTRIM(host_name) = ''",
    "neighbourhood_group": "neighbourhood_group IS NULL OR BTRIM(neighbourhood_group) = ''",
    "neighbourhood": "neighbourhood IS NULL OR BTRIM(neighbourhood) = ''",
    "latitude": "latitude IS NULL",
    "longitude": "longitude IS NULL",
    "room_type": "room_type IS NULL OR BTRIM(room_type) = ''",
    "price": "price IS NULL",
    "minimum_nights": "minimum_nights IS NULL",
    "number_of_reviews": "number_of_reviews IS NULL",
    "reviews_per_month": "reviews_per_month IS NULL",
    "calculated_host_listings_count": "calculated_host_listings_count IS NULL",
    "availability_365": "availability_365 IS NULL",
    "process_date": "process_date IS NULL",
}
OIDC_ISSUER = os.getenv("OIDC_ISSUER", "").rstrip("/")
OIDC_AUDIENCE = os.getenv("OIDC_AUDIENCE", "")
OIDC_JWKS_URL = os.getenv("OIDC_JWKS_URL", "")
OIDC_ROLES_CLAIM = os.getenv("OIDC_ROLES_CLAIM", "roles")
bearer_scheme = HTTPBearer(auto_error=False)
oidc_jwks_client = jwt.PyJWKClient(OIDC_JWKS_URL) if OIDC_JWKS_URL else None
rate_limit_events: dict[str, deque[float]] = defaultdict(deque)
rate_limit_lock = threading.Lock()


class SummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    listing_count: int = Field(ge=0)
    neighbourhood_count: int = Field(ge=0)
    room_type_count: int = Field(ge=0)
    average_price: float | None = Field(default=None, ge=0)
    average_availability: float | None = Field(default=None, ge=0)
    last_loaded_at: str | None = None


class DataQualityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_records: int = Field(ge=0)
    missing_values: int = Field(ge=0)
    duplicate_ids: int = Field(ge=0)
    invalid_prices: int = Field(ge=0)
    validation_status: str


class NeighbourhoodMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    neighbourhood_group: str
    neighbourhood: str
    listing_count: int = Field(ge=0)
    average_price: float = Field(ge=0)
    median_price: float = Field(ge=0)
    average_availability: float = Field(ge=0)


class RoomTypeMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    room_type: str
    listing_count: int = Field(ge=0)
    average_price: float = Field(ge=0)
    average_reviews: float = Field(ge=0)
    average_availability: float = Field(ge=0)


class ListingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    listing_id: int
    name: str
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    room_type: str
    price: float = Field(ge=0)
    minimum_nights: int = Field(ge=0)
    number_of_reviews: int = Field(ge=0)
    availability_365: int = Field(ge=0, le=365)


class PaginatedListings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ListingResponse]
    limit: int
    offset: int


class PipelineStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_id: str
    dag_run_id: str | None = None
    state: str
    last_run_at: str | None = None
    latest_successful_run_at: str | None = None
    data_loaded_at: str | None = None
    freshness: str


class PipelineRunHistoryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_run_id: str
    state: str
    execution_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    failed_task_count: int = Field(ge=0)


class PipelineHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_id: str
    latest_failed: str | None = None
    recent_runs: list[PipelineRunHistoryItem]


class PipelineTriggerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_id: str
    dag_run_id: str
    state: str
    retry_of: str | None = None


@asynccontextmanager
async def lifespan(application: FastAPI):
    if ENVIRONMENT == "production":
        if len(OPERATIONS_API_KEY) < 32:
            raise RuntimeError(
                "OPERATIONS_API_KEY must contain at least 32 characters in production"
            )
        if not AIRFLOW_PASSWORD:
            raise RuntimeError("AIRFLOW_PASSWORD must be configured in production")
        if not OIDC_ISSUER or not OIDC_AUDIENCE or not OIDC_JWKS_URL:
            raise RuntimeError(
                "OIDC_ISSUER, OIDC_AUDIENCE, and OIDC_JWKS_URL are required in production"
            )
    application.state.db_pool = ConnectionPool(
        conninfo=DATABASE_DSN, min_size=1, max_size=10, open=True
    )
    logger.info("Analytics API database pool initialized")
    yield
    application.state.db_pool.close()
    logger.info("Analytics API database pool closed")


app = FastAPI(
    title="Real Estate Analytics API",
    version="1.0.0",
    description="Curated real-estate analytics and protected pipeline operations.",
    lifespan=lifespan,
)


@app.middleware("http")
async def security_middleware(request: Request, call_next) -> Response:
    request_id = str(uuid4())
    if request.url.path.startswith("/api/v1/operations"):
        client_ip = request.client.host if request.client else "unknown"
        now = time.monotonic()
        with rate_limit_lock:
            events = rate_limit_events[client_ip]
            while events and now - events[0] >= 60:
                events.popleft()
            if len(events) >= RATE_LIMIT_PER_MINUTE:
                logger.warning(
                    "audit_event=rate_limit_exceeded client_ip=%s request_id=%s",
                    client_ip,
                    request_id,
                )
                return Response(
                    content='{"detail":"Too many operations requests"}',
                    status_code=429,
                    media_type="application/json",
                    headers={"Retry-After": "60", "X-Request-ID": request_id},
                )
            events.append(now)

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; "
        "base-uri 'self'; "
        "frame-ancestors 'none'; "
        "img-src 'self' data: https://fastapi.tiangolo.com https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "font-src 'self' data: https://cdn.jsdelivr.net; "
        "connect-src 'self' http://localhost:8000 https://cdn.jsdelivr.net; "
        "form-action 'self'"
    )
    if ENVIRONMENT == "production":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    logger.info(
        "request_completed method=%s path=%s status=%s request_id=%s",
        request.method,
        request.url.path,
        response.status_code,
        request_id,
    )
    return response


def get_connection(request: Request):
    with request.app.state.db_pool.connection() as connection:
        yield connection


Connection = Annotated[Any, Depends(get_connection)]


def _local_principal(
    x_operations_key: str | None,
    x_local_role: str | None,
    request: Request,
) -> dict[str, Any] | None:
    if ENVIRONMENT == "production" or not OPERATIONS_API_KEY:
        return None
    if not x_operations_key or not hmac.compare_digest(x_operations_key, OPERATIONS_API_KEY):
        if x_operations_key:
            logger.warning(
                "audit_event=operations_auth_failed path=%s client_ip=%s",
                request.url.path if request else "unknown",
                request.client.host if request and request.client else "unknown",
            )
        return None
    if x_local_role is None:
        return {"subject": "local-admin", "roles": {"admin", "operator", "viewer"}}
    role_name = x_local_role.strip().lower()
    if role_name not in VALID_LOCAL_ROLES:
        logger.warning(
            "audit_event=local_role_invalid path=%s role=%s client_ip=%s",
            request.url.path if request else "unknown",
            x_local_role,
            request.client.host if request and request.client else "unknown",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid local role '{x_local_role}'. Allowed: viewer, operator, admin",
        )
    return {"subject": "local-admin", "roles": {role_name}}


def require_authenticated(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    x_operations_key: str | None = Header(
        default=None,
        alias="X-Operations-Key",
        title="Operations API key",
        description=(
            "Local development operations key used for RBAC testing without a real OIDC "
            "provider. This is not used in production OIDC mode."
        ),
        examples=["local-development-operations-key"],
    ),
    x_local_role: str | None = Header(
        default=None,
        alias="X-Local-Role",
        title="Local role",
        description=(
            "Local RBAC override for development testing. Allowed values: viewer, operator, "
            "admin. This header is only for local validation and does not affect production "
            "OIDC authentication."
        ),
        examples=["viewer", "operator", "admin"],
    ),
) -> dict[str, Any]:
    local_principal = _local_principal(x_operations_key, x_local_role, request)
    if local_principal:
        return local_principal
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not oidc_jwks_client or not OIDC_ISSUER or not OIDC_AUDIENCE:
        raise HTTPException(status_code=503, detail="OIDC authentication is not configured")
    try:
        signing_key = oidc_jwks_client.get_signing_key_from_jwt(credentials.credentials)
        claims = jwt.decode(
            credentials.credentials,
            signing_key.key,
            algorithms=["RS256"],
            audience=OIDC_AUDIENCE,
            issuer=OIDC_ISSUER,
        )
    except (jwt.PyJWTError, jwt.exceptions.PyJWKClientError) as exc:
        logger.warning("audit_event=oidc_auth_failed path=%s", request.url.path)
        raise HTTPException(status_code=401, detail="Invalid bearer token") from exc
    raw_roles = claims.get(OIDC_ROLES_CLAIM, [])
    roles = {raw_roles} if isinstance(raw_roles, str) else set(raw_roles)
    return {"subject": claims.get("sub", "unknown"), "roles": roles}


def require_viewer(
    principal: Annotated[dict[str, Any], Depends(require_authenticated)],
) -> dict[str, Any]:
    if not principal.get("roles") or not principal["roles"] & {"viewer", "operator", "admin"}:
        raise HTTPException(status_code=403, detail="Viewer role required")
    return principal


def require_operator(
    principal: Annotated[dict[str, Any], Depends(require_authenticated)],
) -> dict[str, Any]:
    if not principal.get("roles") or not principal["roles"] & {"operator", "admin"}:
        raise HTTPException(status_code=403, detail="Operator role required")
    return principal


ViewerAccess = Annotated[dict[str, Any], Depends(require_viewer)]
OperatorAccess = Annotated[dict[str, Any], Depends(require_operator)]


def execute_one(
    connection: Any, query: str, parameters: tuple[Any, ...] | None = None
) -> dict[str, Any]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, parameters) if parameters is not None else cursor.execute(query)
        row = cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=503, detail="Analytics tables are not populated")
    return dict(row)


def airflow_request(method: str, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
    if not AIRFLOW_PASSWORD:
        raise HTTPException(status_code=503, detail="Airflow API credentials are not configured")
    try:
        response = httpx.request(
            method,
            f"{AIRFLOW_API_URL.rstrip('/')}/{path.lstrip('/')}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            json=json,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        logger.exception("Airflow API request failed with status %s", exc.response.status_code)
        raise HTTPException(status_code=502, detail="Airflow API request failed") from exc
    except httpx.HTTPError as exc:
        logger.exception("Airflow API request failed")
        raise HTTPException(status_code=503, detail="Airflow API unavailable") from exc


@app.get("/health/live", tags=["health"])
def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
def readiness(connection: Connection) -> dict[str, str]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM real_estate_listings LIMIT 1")
    except OperationalError as exc:
        logger.exception("Analytics database readiness check failed")
        raise HTTPException(status_code=503, detail="Analytics database unavailable") from exc
    return {"status": "ok"}


@app.get(
    "/api/v1/operations/pipeline",
    response_model=PipelineStatusResponse,
    tags=["operations"],
)
def pipeline_status(connection: Connection, _: ViewerAccess) -> PipelineStatusResponse:
    airflow = airflow_request(
        "GET", f"/dags/{PIPELINE_DAG_ID}/dagRuns?limit=100&order_by=-execution_date"
    )
    runs = airflow.get("dag_runs") or []
    latest_run = runs[0] if runs else None
    latest_successful_run = next(
        (run for run in runs if str(run.get("state", "")).lower() == "success"), None
    )
    loaded_at = execute_one(
        connection,
        "SELECT MAX(loaded_at)::text AS data_loaded_at FROM real_estate_listings",
    ).get("data_loaded_at")
    freshness = "unknown"
    if loaded_at:
        loaded_datetime = datetime.fromisoformat(loaded_at.replace("Z", "+00:00"))
        age_hours = (datetime.now(UTC) - loaded_datetime).total_seconds() / 3600
        freshness = "fresh" if age_hours <= FRESHNESS_THRESHOLD_HOURS else "stale"
    return PipelineStatusResponse(
        dag_id=PIPELINE_DAG_ID,
        dag_run_id=latest_run.get("dag_run_id") if latest_run else None,
        state=latest_run.get("state", "never_run") if latest_run else "never_run",
        last_run_at=latest_run.get("execution_date") if latest_run else None,
        latest_successful_run_at=(
            latest_successful_run.get("end_date") or latest_successful_run.get("execution_date")
            if latest_successful_run
            else None
        ),
        data_loaded_at=loaded_at,
        freshness=freshness,
    )


@app.get(
    "/api/v1/operations/pipeline/history",
    response_model=PipelineHistoryResponse,
    tags=["operations"],
)
def pipeline_history(_: ViewerAccess) -> PipelineHistoryResponse:
    airflow = airflow_request(
        "GET", f"/dags/{PIPELINE_DAG_ID}/dagRuns?limit=10&order_by=-execution_date"
    )
    runs = airflow.get("dag_runs") or []
    recent_runs: list[PipelineRunHistoryItem] = []
    latest_failed: str | None = None

    for run in runs:
        dag_run_id = run.get("dag_run_id")
        if not dag_run_id:
            continue
        task_instances = (
            airflow_request(
                "GET",
                f"/dags/{PIPELINE_DAG_ID}/dagRuns/{dag_run_id}/taskInstances?limit=200",
            ).get("task_instances")
            or []
        )
        failed_task_count = sum(
            1 for task in task_instances if str(task.get("state", "")).lower() == "failed"
        )
        recent_runs.append(
            PipelineRunHistoryItem(
                dag_run_id=dag_run_id,
                state=str(run.get("state", "unknown")),
                execution_date=run.get("execution_date"),
                start_date=run.get("start_date"),
                end_date=run.get("end_date"),
                failed_task_count=failed_task_count,
            )
        )
        if latest_failed is None and str(run.get("state", "")).lower() == "failed":
            latest_failed = dag_run_id

    return PipelineHistoryResponse(
        dag_id=PIPELINE_DAG_ID,
        latest_failed=latest_failed,
        recent_runs=recent_runs,
    )


@app.post(
    "/api/v1/operations/pipeline/run",
    response_model=PipelineTriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["operations"],
)
def trigger_pipeline(_: OperatorAccess) -> PipelineTriggerResponse:
    run_id = f"api__{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    result = airflow_request(
        "POST",
        f"/dags/{PIPELINE_DAG_ID}/dagRuns",
        {"dag_run_id": run_id, "conf": {"source": "analytics_api"}},
    )
    return PipelineTriggerResponse(
        dag_id=PIPELINE_DAG_ID,
        dag_run_id=result.get("dag_run_id", run_id),
        state=result.get("state", "queued"),
    )


@app.post(
    "/api/v1/operations/pipeline/retry",
    response_model=PipelineTriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["operations"],
)
def retry_pipeline(
    dag_run_id: str = Query(..., min_length=1, max_length=250),
    _: OperatorAccess = None,
) -> PipelineTriggerResponse:
    source_run = airflow_request(
        "GET", f"/dags/{PIPELINE_DAG_ID}/dagRuns/{quote(dag_run_id, safe='')}"
    )
    source_state = str(source_run.get("state", "")).lower()
    if source_state != "failed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only failed pipeline runs can be retried",
        )

    retry_run_id = f"api_retry__{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    result = airflow_request(
        "POST",
        f"/dags/{PIPELINE_DAG_ID}/dagRuns",
        {
            "dag_run_id": retry_run_id,
            "conf": {"source": "analytics_api_retry", "retry_of": dag_run_id},
        },
    )
    return PipelineTriggerResponse(
        dag_id=PIPELINE_DAG_ID,
        dag_run_id=result.get("dag_run_id", retry_run_id),
        state=result.get("state", "queued"),
        retry_of=dag_run_id,
    )


@app.get("/api/v1/summary", response_model=SummaryResponse, tags=["analytics"])
def summary(connection: Connection) -> SummaryResponse:
    row = execute_one(
        connection,
        """
        SELECT COUNT(*)::int AS listing_count,
               (SELECT COUNT(*)::int FROM real_estate_neighbourhood_metrics) AS neighbourhood_count,
               (SELECT COUNT(*)::int FROM real_estate_room_type_metrics) AS room_type_count,
               ROUND(AVG(price), 2)::float AS average_price,
               ROUND(AVG(availability_365), 1)::float AS average_availability,
               MAX(loaded_at)::text AS last_loaded_at
        FROM real_estate_listings
        """,
    )
    return SummaryResponse(**row)


@app.get("/api/v1/data-quality", response_model=DataQualityResponse, tags=["analytics"])
def data_quality(connection: Connection) -> DataQualityResponse:
    missing_columns = ",\n".join(
        f"COUNT(*) FILTER (WHERE {expression})::int AS missing_{column}"
        for column, expression in DATA_QUALITY_MISSING_EXPRESSIONS.items()
    )
    missing_total = " + ".join(
        f"COUNT(*) FILTER (WHERE {expression})"
        for expression in DATA_QUALITY_MISSING_EXPRESSIONS.values()
    )
    row = execute_one(
        connection,
        f"""
        SELECT COUNT(*)::int AS total_records,
               ({missing_total})::int AS missing_values,
               {missing_columns},
               (COUNT(*) - COUNT(DISTINCT listing_id))::int AS duplicate_ids,
               COUNT(*) FILTER (
                   WHERE price IS NULL OR price < %s OR price > %s
               )::int AS invalid_prices
        FROM real_estate_listings
        """,
        (DATA_QUALITY_MIN_PRICE, DATA_QUALITY_MAX_PRICE),
    )
    total_records = row["total_records"]
    missing_counts = {
        column: row[f"missing_{column}"] for column in DATA_QUALITY_MISSING_EXPRESSIONS
    }
    if total_records == 0:
        validation_status = "empty"
    elif (
        row["duplicate_ids"]
        or row["invalid_prices"]
        or missing_counts["listing_id"]
        or missing_counts["price"]
        or any(
            count / total_records > DATA_QUALITY_MAX_NULL_RATE for count in missing_counts.values()
        )
    ):
        validation_status = "failed"
    else:
        validation_status = "passed"

    return DataQualityResponse(
        total_records=total_records,
        missing_values=row["missing_values"],
        duplicate_ids=row["duplicate_ids"],
        invalid_prices=row["invalid_prices"],
        validation_status=validation_status,
    )


@app.get(
    "/api/v1/metrics/neighbourhoods",
    response_model=list[NeighbourhoodMetric],
    tags=["analytics"],
)
def neighbourhood_metrics(
    connection: Connection,
    neighbourhood_group: str | None = Query(default=None, min_length=1, max_length=100),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[NeighbourhoodMetric]:
    query = """
        SELECT neighbourhood_group, neighbourhood, listing_count, average_price,
               median_price, average_availability
        FROM real_estate_neighbourhood_metrics
    """
    parameters: list[Any] = []
    if neighbourhood_group:
        query += " WHERE neighbourhood_group = %s"
        parameters.append(neighbourhood_group)
    query += " ORDER BY listing_count DESC LIMIT %s"
    parameters.append(limit)
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, parameters)
        return [NeighbourhoodMetric(**dict(row)) for row in cursor.fetchall()]


@app.get("/api/v1/metrics/room-types", response_model=list[RoomTypeMetric], tags=["analytics"])
def room_type_metrics(connection: Connection) -> list[RoomTypeMetric]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT room_type, listing_count, average_price, average_reviews,
                   average_availability
            FROM real_estate_room_type_metrics
            ORDER BY listing_count DESC
            """
        )
        return [RoomTypeMetric(**dict(row)) for row in cursor.fetchall()]


@app.get("/api/v1/listings", response_model=PaginatedListings, tags=["listings"])
def listings(
    connection: Connection,
    neighbourhood_group: str | None = Query(default=None, min_length=1, max_length=100),
    room_type: str | None = Query(default=None, min_length=1, max_length=100),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PaginatedListings:
    query = """
        SELECT listing_id, name, host_name, neighbourhood_group, neighbourhood,
               room_type, price, minimum_nights, number_of_reviews, availability_365
        FROM real_estate_listings
    """
    conditions: list[str] = []
    parameters: list[Any] = []
    if neighbourhood_group:
        conditions.append("neighbourhood_group = %s")
        parameters.append(neighbourhood_group)
    if room_type:
        conditions.append("room_type = %s")
        parameters.append(room_type)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY listing_id LIMIT %s OFFSET %s"
    parameters.extend([limit, offset])
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, parameters)
        items = [ListingResponse(**dict(row)) for row in cursor.fetchall()]
    return PaginatedListings(items=items, limit=limit, offset=offset)
