
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

def build_avs_submission(shots):
    """Build a DRES submission containing multiple temporal answers."""

    return {
        "answerSets": [
            {
                "answers": [
                    {
                        "mediaItemName": shot.video_id,
                        "start": shot.start_ms,
                        "end": shot.end_ms,
                    }
                    for shot in shots
                ]
            }
        ]
    }


def submit_to_dres(payload, evaluation_id):
    """Send a submission to the selected DRES evaluation."""

    base_url = os.environ["DRES_BASE_URL"].rstrip("/")
    session_token = os.environ["DRES_SESSION_TOKEN"]

    if not isinstance(evaluation_id, str) or not evaluation_id.strip():
        raise ValueError("A DRES evaluation ID is required.")

    evaluation_id = evaluation_id.strip()

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
        return error.code, error.read().decode("utf-8")

    except URLError as error:
        raise ConnectionError("Unable to connect to DRES") from error

