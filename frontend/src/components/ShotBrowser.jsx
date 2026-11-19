
import { useEffect, useState } from "react";
import { Alert, Button, Col, Row, Spinner } from "react-bootstrap";
import ShotThumbnail from "./ShotThumbnail";

export default function ShotBrowser({ videoId, suggestedShotId, onClose, onSelectShot }) {
    const [shots, setShots] = useState([]);
    const [title, setTitle] = useState("");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    useEffect(() => {
        if (!videoId) return;

        const controller = new AbortController();

        async function loadShots() {
            setLoading(true);
            setError("");
            setShots([]);

            try {
                const response = await fetch(`/api/videos/${videoId}/shots/`, { signal: controller.signal });

                if (!response.ok) {
                    throw new Error("Failed to load video shots");
                }

                const data = await response.json();
                const loadedShots = data.shots || [];

                setShots(loadedShots);
                setTitle(data.title || videoId);

                if (suggestedShotId) {
                    const suggestedShot = loadedShots.find((shot) => shot.shot_id === suggestedShotId);

                    if (suggestedShot && !controller.signal.aborted) {
                        onSelectShot(suggestedShot);
                    }
                }

            } catch (err) {
                if (err.name !== "AbortError") {
                    setError(err.message);
                }
            } finally {
                if (!controller.signal.aborted) {
                    setLoading(false);
                }
            }
        }

        loadShots();

        return () => controller.abort();
    }, [videoId, suggestedShotId]);

    if (!videoId) return null;

    return (
        <section className="mt-4">
        <div className="d-flex justify-content-between align-items-center mb-3">
            <h3 className="mb-0">Shots: {title || videoId}</h3>
            <Button variant="light" onClick={onClose}> Close</Button>
        </div>

        {loading && <Spinner animation="border" />}

        {error && <Alert variant="danger">{error}</Alert>}

        {!loading && !error && (
            <div className="mt-4">
                <p>{shots.length} shots</p>

                <Row xs={1} sm={2} md={3} lg={4} className="g-3">             
                    {shots.map((shot) => {
                        const isSuggested = shot.shot_id === suggestedShotId;

                        return (
                            <Col key={shot.shot_id}>
                                <div className="position-relative h-100">
                                    {isSuggested && (
                                        <span className="badge bg-success position-absolute top-0 start-0 m-2" style={{ zIndex: 1 }}>
                                            Suggested match
                                        </span>
                                    )}

                                    <div className={`h-100 rounded ${isSuggested ? "border border-success border-3" : ""}`}>
                                        <ShotThumbnail shot={shot} onSelect={onSelectShot}/>
                                    </div>
                                </div>
                            </Col>
                        );
                    })}
                </Row>
            </div>
        )}
        </section>
    );
}
