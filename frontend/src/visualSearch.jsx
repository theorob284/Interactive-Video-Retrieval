
import { useState } from "react";
import { Container, Form, Button, Table, Alert, Spinner } from "react-bootstrap";
import VideoPlayer from "./components/VideoPlayer";

export default function VisualSearch() {
    const [file, setFile] = useState(null);
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [selectedShot, setSelectedShot] = useState(null);

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
        <Container className="mt-4">
        <h2>Visual Search</h2>

        <p>Upload an image or video to search for matching shots.</p>

        <Form onSubmit={handleSearch}>
            <Form.Group className="mb-3">
            <Form.Control type="file" accept=".jpg,.jpeg,.png,.webp,.mp4,.mov" onChange={(event) => {setFile(event.target.files[0] || null);}}/>
            </Form.Group>

            <Button type="submit" disabled={!file || loading}>
            {loading ? "Searching..." : "Search"}
            </Button>
        </Form>

        {loading && <Spinner animation="border" className="mt-3" />}

        {error && (<Alert variant="danger" className="mt-3"> {error} </Alert>)}

        {selectedShot && (<VideoPlayer videoId={selectedShot.video_id} shot={{shot_id: selectedShot.shotID, start_ms: selectedShot.start_ms, end_ms: selectedShot.end_ms}}/>)}

        {!loading && results.length > 0 && (
            <Table striped bordered hover className="mt-4">
            <thead>
                <tr>
                <th>Rank</th>
                <th>Shot ID</th>
                <th>Distance</th>
                <th>Matched Frames</th>
                <th>Action</th>
                </tr>
            </thead>

            <tbody>
                {results.map((result, index) => (
                <tr key={result.shotID}>
                    <td>{index + 1}</td>
                    <td>{result.shotID}</td>
                    <td>{result.distance}</td>
                    <td>{result.matchedFrames}</td>
                    <td><Button variant="outline-primary" size="sm" onClick={() => setSelectedShot(result)}> Select Shot</Button></td>
                </tr>
                ))}
            </tbody>
            </Table>
        )}

        {!loading && !error && results.length === 0 && ( <p className="mt-3 text-muted"> Upload a file to see visual search results.</p>)}
        </Container>
    );
}
