
import { useEffect, useRef, useState } from "react";
import { Alert, Card } from "react-bootstrap";
import DresSubmission from "../DresSubmission";

export default function VideoPlayer({ videoId, shot }) {
    const videoRef = useRef(null);
    const [error, setError] = useState("");

    useEffect(() => {
        setError("");

        const player = videoRef.current;

        if (!player || !shot) return;

        function seekToShot() {
            player.currentTime = shot.start_ms / 1000;
        }

        if (player.readyState >= 1) { 
            seekToShot();
        } else {
            player.addEventListener("loadedmetadata", seekToShot, { once: true });
            return () => {player.removeEventListener("loadedmetadata", seekToShot);};
        }
    }, [videoId, shot]);

    if (!videoId || !shot) return null;

    return (
        <Card className="mt-4 mb-4">
        <Card.Body>
            <Card.Title>Selected Shot: {shot.shot_id}</Card.Title>

            <p>Start: {(shot.start_ms / 1000).toFixed(2)}s {" | "} End: {(shot.end_ms / 1000).toFixed(2)}s</p>

            <video key={videoId} ref={videoRef} controls preload="metadata" style={{ width: "100%", maxHeight: "500px" }} src={`/api/videos/${videoId}/file/`}
            onError={() => setError("Video could not be played. Check the file path and browser codec support.")}/>

            <DresSubmission shot={shot} />

            {error && <Alert variant="warning" className="mt-3">{error}</Alert>}
        </Card.Body>
        </Card>
    );
}
