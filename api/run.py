import os
import uvicorn
from app.main import app

import sentry_sdk

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=os.getenv("SENTRY_ENV"),
    send_default_pii=True,
    traces_sample_rate=1.0,
    # To collect profiles for all profile sessions,
    # set `profile_session_sample_rate` to 1.0.
    profile_session_sample_rate=1.0,
    # Profiles will be automatically collected while
    # there is an active span.
    profile_lifecycle="trace",
    attach_stacktrace=True,
)

port = os.getenv("SERVER_PORT", "5000")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(port))
