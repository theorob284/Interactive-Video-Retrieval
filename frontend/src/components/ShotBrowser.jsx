
import { useEffect, useState } from "react";
import { Alert, Button, Col, Row, Spinner } from "react-bootstrap";
import ShotThumbnail from "./ShotThumbnail";

export default function ShotBrowser({ videoId, onClose, onSelectShot }) {
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

                setShots(data.shots || []);
                setTitle(data.title || videoId);
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
    }, [videoId]);

    if (!videoId) return null;

    return (
        <section className="mt-4">
        <div className="d-flex justify-content-between align-items-center mb-3">
            <h3 className="mb-0">Shots: {title || videoId}</h3>
            <Button variant="outline-secondary" onClick={onClose}> Close</Button>
        </div>

        {loading && <Spinner animation="border" />}

        {error && <Alert variant="danger">{error}</Alert>}

        {!loading && !error && (
            <>
                <p>{shots.length} shots</p>

                <Row xs={1} sm={2} md={3} lg={4} className="g-3">
                    {shots.map((shot) => (
                    <Col key={shot.shot_id}>
                        <ShotThumbnail
                        shot={shot}
                        onSelect={onSelectShot}
                        />
                    </Col>
                    ))}
                </Row>
            </>
        )}
        </section>
    );
}
