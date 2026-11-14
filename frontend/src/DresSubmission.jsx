
import { useEffect, useState } from "react";
import { Alert, Button, Card, Spinner } from "react-bootstrap";

function getCookie(name) {
    const cookie = document.cookie.split("; ").find((item) => item.startsWith(`${name}=`));
    return cookie ? decodeURIComponent(cookie.substring(name.length + 1)) : "";
}

export default function DresSubmission({ shot }) {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [preview, setPreview] = useState(null);
    const [submission, setSubmission] = useState(null);
    const LIVE_ENABLED = true;

    useEffect(() => {
        setPreview(null);
        setSubmission(null);
        setError("");
    }, [shot?.shot_id]);

    async function sendRequest(submit) {
        if (!shot) return;

        setLoading(true);
        setError("");

        if (submit) {
            setSubmission(null);
        } else {
            setPreview(null);
        }

        try {
            const csrfResponse = await fetch("/api/csrf/", {credentials: "same-origin",});

        if (!csrfResponse.ok) {
            throw new Error("Could not initialise CSRF protection");
        }

        const response = await fetch("/api/dres/submit/", {
            method: "POST",
            credentials: "same-origin",
            headers: {"Content-Type": "application/json", "X-CSRFToken": getCookie("csrftoken"),},
            body: JSON.stringify({
            shot_id: shot.shot_id,
            submit,
            }),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "DRES request failed");
        }

        if (submit) {
            setSubmission(data);
        } else {
            setPreview(data.payload);
        }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }

    if (!shot) return null;

    return (
        <Card className="mt-3">
        <Card.Body>
            <Card.Title>DRES Submission</Card.Title>
            <p className="mb-3">Selected shot: <strong>{shot.shot_id}</strong></p>

            <div className="d-flex gap-2 flex-wrap">
                <Button variant="outline-primary" disabled={loading} onClick={() => sendRequest(false)}> Preview Submission</Button>
                <Button variant="success" disabled={loading || !LIVE_ENABLED} onClick={() => sendRequest(true)}> Submit to DRES</Button>
            </div>

            {loading && (<Spinner animation="border" size="sm" className="mt-3" />)}

            {error && (<Alert variant="danger" className="mt-3">{error}</Alert>)}

            {preview && (
                <Alert variant="info" className="mt-3">
                    <strong>Submission preview generated.</strong>
                    <p className="mb-2">Nothing has been sent to DRES.</p>
                    <pre className="mb-0" style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(preview, null, 2)}</pre>
                </Alert>
            )}

            {submission && (
                <Alert variant={submission.accepted ? "success" : "warning"}className="mt-3">
                    <strong>DRES response received</strong>
                    <pre className="mb-0 mt-2" style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(submission.response, null, 2)}</pre>
                </Alert>
            )}
        </Card.Body>
        </Card>
    );
}
