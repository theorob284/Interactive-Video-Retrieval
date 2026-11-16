
import { useState, useEffect, useRef} from "react";
import {Row, Col} from "react-bootstrap";
import { Container, Form, Button, Table, Alert, Spinner } from "react-bootstrap";
import VideoPlayer from "./components/VideoPlayer";
import ShotThumbnail from "./components/ShotThumbnail";

export default function VisualSearch() {
    const [file, setFile] = useState(null);
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [selectedShot, setSelectedShot] = useState(null);
    const videoPlayerRef = useRef(null);

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

    useEffect(() => {
        if (selectedShot && videoPlayerRef.current) {
            videoPlayerRef.current.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
        }
    }, [selectedShot]);
    
    async function handleSearch(event) {
        event.preventDefault();

        if (!file) return;

        setLoading(true);
        setError("");
        setResults([]);
        setSelectedShot(null);

        const formData = new FormData();
        formData.append("file", file);

        try {
            // Get the CSRF cookie first
            await fetch("/api/csrf/", {
                credentials: "same-origin",
            });

            // Submit the image or video
            const response = await fetch("/api/search/visual/", {
                method: "POST",
                credentials: "same-origin",
                headers: {"X-CSRFToken": getCookie("csrftoken"),},
                body: formData,
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || "Visual search failed");
            }

            setResults(data.results || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }


    return (
        <Container className="mt-3">
        <h2>Visual Search (KIS-V) - Shot Search</h2>
        <p>Upload an image or video to search for matching shots.</p>

        <Form className="d-flex gap-2 search-form" onSubmit={handleSearch}>
            <Form.Group className="flex-grow-1">
                <Form.Control type="file" accept=".jpg,.jpeg,.png,.webp,.mp4,.mov" onChange={(event) => {setFile(event.target.files[0] || null);}}/>
            </Form.Group>

            <Button type="submit" disabled={!file || loading}>
                {loading ? "Searching..." : "Search"}
            </Button>
        </Form>

        {loading && <Spinner animation="border" className="mt-3" />}

        {error && (<Alert variant="danger" className="mt-3"> {error} </Alert>)}

        {selectedShot && (
            <div ref={videoPlayerRef} className="video-player-section">
                <VideoPlayer videoId={selectedShot.video_id} shot={{shot_id: selectedShot.shotID, start_ms: selectedShot.start_ms, end_ms: selectedShot.end_ms}}/>
            </div>  
        )}
    
        {!loading && results.length > 0 && (
            <div className="mt-4 mb-2">
                <h4 className="mb-3">Visual Search Results ({results.length})</h4>

                <Row xs={1} sm={2} md={3} lg={4} className="g-3">
                    {results.map((result, index) => (
                        <Col key={result.shotID}>
                            <div className="h-100">
                                <ShotThumbnail
                                    shot={{
                                        shot_id: result.shotID,
                                        start_ms: result.start_ms,
                                        end_ms: result.end_ms,
                                        thumbnail_url: result.thumbnail_url,
                                    }}
                                    rank={index + 1}
                                    scoreLabel={
                                    result.distance != null
                                        ? `Distance: ${result.distance}`
                                        : `Score: ${result.score?.toFixed(2) ?? "—"}`
                                    }
                                    onSelect={() => setSelectedShot(result)}
                                />
                            </div>
                        </Col>
                    ))}
                </Row>
            </div>
        )}


        {!loading && !error && results.length === 0 && ( <p className="mt-3 text-muted"> Upload a file to see visual search results.</p>)}
        </Container>
    );
}
