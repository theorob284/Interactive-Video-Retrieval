
import json
import os
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

def build_submission(shot):
    """Build a DRES temporal submission from a Django Shot."""

    return {
        "answerSets": [
            {
                "answers": [
                    {
                        "mediaItemName": shot.video_id,
                        "start": shot.start_ms,
                        "end": shot.end_ms,
                    }
                ]
            }
        ]
    }

def submit_to_dres(payload):
    """Send a submission to the current DRES evaluation."""

    base_url = os.environ["DRES_BASE_URL"].rstrip("/")
    evaluation_id = os.environ["DRES_EVALUATION_ID"]
    session_token = os.environ["DRES_SESSION_TOKEN"]

    url = (
        f"{base_url}/api/v2/submit/"
        f"{quote(evaluation_id, safe='')}?"
        f"{urlencode({'session': session_token})}"
    )

    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "DCU-InteractiveVideoRetrieval/1.0",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8")
            return response.status, body

    except HTTPError as error:
        print("DRES HTTP status:", error.code)
        print("Response server:", error.headers.get("Server"))
        print("Cloudflare Ray ID:", error.headers.get("CF-Ray"))
        return error.code, error.read().decode("utf-8")

    except URLError as error:
        raise ConnectionError("Unable to connect to DRES") from error
