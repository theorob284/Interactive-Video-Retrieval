
import { useState, useEffect, useRef } from "react";
import { Container, Form, Button, Row, Col, Alert, Spinner } from "react-bootstrap";
import ShotThumbnail from "./components/ShotThumbnail";
import VideoPlayer from "./components/VideoPlayer";

export default function AVS() {
    const [query, setQuery] = useState("");
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const [selectedShot, setSelectedShot] = useState(null);
    const [selectedShots, setSelectedShots] = useState([]);

    const [preview, setPreview] = useState(null);
    const [submission, setSubmission] = useState(null);
    const [submitting, setSubmitting] = useState(false);
    const [submissionError, setSubmissionError] = useState("");
    const [evaluationId, setEvaluationId] = useState("");


    const videoPlayerRef = useRef(null);

    useEffect(() => {
        if (selectedShot && videoPlayerRef.current) {
            videoPlayerRef.current.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
        }
    }, [selectedShot]);

    function getCookie(name) {
        const cookies = document.cookie.split(";");

        for (const cookie of cookies) {
            const [key, ...value] = cookie.trim().split("=");

            if (key === name) {
                return decodeURIComponent(value.join("="));
            }
        }

        return null;
    }

    async function handleSearch(event) {
        event.preventDefault();

        if (!query.trim()) return;

        setLoading(true);
        setError("");
        setResults([]);
        setSelectedShot(null);

        try {
            const response = await fetch(`/api/search/avs/?q=${encodeURIComponent(query)}`);

            if (!response.ok) {
                throw new Error("AVS search failed");
            }

            const data = await response.json();
            setResults(data.results || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }

    function toggleShot(shot) {
        setSelectedShots((current) => {
            const exists = current.some((item) => item.shot_id === shot.shot_id);

            return exists
                ? current.filter((item) => item.shot_id !== shot.shot_id)
                : [...current, shot];
        });

        setPreview(null);
        setSubmission(null);
        setSubmissionError("");
    }

    function isShotSelected(shot) {
        return selectedShots.some(
            (item) => item.shot_id === shot.shot_id
        );
    }

    async function handleSubmission(submit = false) {
        if (selectedShots.length === 0) return;

        if (submit && !window.confirm("Submit selected shots to DRES?")) {
            return;
        }

        setSubmitting(true);
        setSubmissionError("");

        try {
            await fetch("/api/csrf/", {credentials: "same-origin",});

            const response = await fetch("/api/submit/avs/", {
                method: "POST",
                credentials: "same-origin",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCookie("csrftoken"),
                },
                body: JSON.stringify({shot_ids: selectedShots.map((shot) => shot.shot_id), submit, evaluation_id: evaluationId.trim(),}),
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || "DRES request failed");
            }

            if (submit) {
                setSubmission(data);
                setPreview(null);
            } else {
                setPreview(data.payload);
                setSubmission(null);
            }
        } catch (err) {
            setSubmissionError(err.message);
        } finally {
            setSubmitting(false);
        }
    }

    return (
        <Container className="mt-3 mb-4">
            <h2>Ad-hoc Video Search (AVS)</h2>
            <p>Search for and select multiple relevant video shots.</p>

            <Form className="d-flex gap-2 search-form" onSubmit={handleSearch}>
                <Form.Control type="text" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Describe the videos you're looking for..."/>

                <Button type="submit" disabled={loading}>
                    {loading ? "Searching..." : "Search"}
                </Button>
            </Form>

            {loading && (
                <Spinner animation="border" className="mt-3" />
            )}

            {error && (
                <Alert variant="danger" className="mt-3">
                    {error}
                </Alert>
            )}

            {/* Selected shot playback */}
            {selectedShot && (
                <div ref={videoPlayerRef} className="video-player-section mt-4">
                    <VideoPlayer videoId={selectedShot.video_id} shot={selectedShot} showDres={false}/>
                    <div className="d-flex mt-2">
                        <Button variant={isShotSelected(selectedShot) ? "success" : "primary"} disabled={submitting} onClick={() => toggleShot(selectedShot)}>
                            {isShotSelected(selectedShot) ? "Added to Submission ✓": "Add to Submission"}
                        </Button>
                    </div>
                </div>
            )}

            {/* AVS submission basket */}
            {selectedShots.length > 0 && (
                <div className="card mt-4">
                    <div className="card-body">
                        <h4 className="mb-3">
                            AVS Submission ({selectedShots.length} shots)
                        </h4>

                        {selectedShots.map((shot) => (
                            <div key={shot.shot_id} className="d-flex justify-content-between align-items-center border-bottom py-2">
                                <div>
                                    <strong>{shot.shot_id}</strong>
                                    <div className="small text-muted">
                                        Video {shot.video_id} · {" "} {(shot.start_ms / 1000).toFixed(2)}s - {" "} {(shot.end_ms / 1000).toFixed(2)}s
                                    </div>
                                </div>

                                <Button variant="light" size="sm" disabled={submitting} onClick={() => toggleShot(shot)}> Remove</Button>
                            </div>
                        ))}

                        <Form.Group className="mt-3" controlId="avsEvaluationId">
                            <Form.Label>DRES Evaluation ID</Form.Label>
                            <Form.Control type="text" placeholder="Enter evaluation/run ID" value={evaluationId} onChange={(e) => {setEvaluationId(e.target.value); setSubmission(null)}}autoComplete="off"disabled={submitting}/>
                        </Form.Group>

                        <div className="d-flex gap-2 mt-3">
                            <Button variant="primary" disabled={submitting} onClick={() => handleSubmission(false)}>Preview Submission</Button>     
                            <Button variant="success" disabled={submitting || !preview || !evaluationId.trim()} onClick={() => handleSubmission(true)}>{submitting ? "Processing..." : "Submit to DRES"}</Button>
                        </div>

                        {submissionError && (
                            <Alert variant="danger" className="mt-3">
                                {submissionError}
                            </Alert>
                        )}

                        {preview && (
                            <Alert variant="info" dismissible className="mt-3" onClose={() => setPreview(null)}>
                                <strong>Submission preview generated.</strong>
                                <p className="mb-2">
                                    Nothing has been sent to DRES.
                                </p>

                                <pre className="mb-0" style={{ whiteSpace: "pre-wrap" }}>
                                    {JSON.stringify(preview, null, 2)}
                                </pre>
                            </Alert>
                        )}

                        {submission && (
                            <Alert
                                variant={ submission.accepted ? "success" : "warning"} dismissible className="mt-3" onClose={() => setSubmission(null)}>
                                <strong>{submission.accepted ? "DRES submission accepted.": "DRES submission not accepted."}</strong>
                                <p className="mb-2">
                                    HTTP status: {submission.dres_status}
                                </p>

                                <pre className="mb-0" style={{ whiteSpace: "pre-wrap" }}>
                                    {JSON.stringify(submission.response, null, 2)}
                                </pre>
                            </Alert>
                        )}
                    </div>
                </div>
            )}

            {/* Search results */}
            {!loading && results.length > 0 && (
                <div className="mt-4">
                    <h4 className="mb-3">
                        AVS Results ({results.length})
                    </h4>

                    <Row xs={1} sm={2} md={3} lg={4} className="g-3">
                        {results.map((shot) => (
                            <Col key={shot.shot_id}>
                                <ShotThumbnail shot={shot} onSelect={setSelectedShot} extraAction={{selected: isShotSelected(shot), disabled: submitting, onClick: toggleShot,}}/>
                            </Col>
                        ))}
                    </Row>

                </div>
            )}
        </Container>
    );
}
