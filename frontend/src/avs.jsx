
import { useState } from "react";
import {Alert, Button, Col, Container, Form, Row, Spinner} from "react-bootstrap";

import ShotThumbnail from "./components/ShotThumbnail";
import VideoPlayer from "./components/VideoPlayer";

export default function AVS() {
    const [query, setQuery] = useState("");
    const [results, setResults] = useState([]);
    const [selectedShot, setSelectedShot] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

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

    return (
        <Container className="mt-4">
        <h2>Ad-hoc Video Search</h2>
        <p>Search for relevant shots across the video collection.</p>

        <Form onSubmit={handleSearch} className="mb-4">
            <Form.Control type="text" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Describe the content you're looking for..." className="mb-2"/>
            <Button type="submit" disabled={loading}> {loading ? "Searching..." : "Search"}</Button>
        </Form>

        {error && <Alert variant="danger">{error}</Alert>}
        {loading && <Spinner animation="border" />}

        {selectedShot && (<VideoPlayer videoId={selectedShot.video_id} shot={selectedShot}/>)}

        {!loading && results.length > 0 && (
            <Row xs={1} sm={2} md={3} lg={4} className="g-3">
            {results.map((shot) => (
                <Col key={shot.shot_id}>
                    <ShotThumbnail shot={shot} onSelect={setSelectedShot}/>
                </Col>
            ))}
            </Row>
        )}
        </Container>
    );
}
