
import { Card, Button } from "react-bootstrap";

export default function VideoCard({ video, onSelect }) {
    return (
        <Card className="mb-3">
        <Card.Body>
            <Card.Title>{video.title || video.video_id}</Card.Title>

            <Card.Text>
            <strong>Video ID:</strong> {video.video_id}
            <br />
            <strong>BM25 Score:</strong> {video.score.toFixed(2)}
            <br />
            <strong>Shots:</strong> {video.shot_count}
            </Card.Text>

            <Button variant="primary" onClick={() => onSelect(video.video_id)}>
            Browse Shots
            </Button>
        </Card.Body>
        </Card>
    );
}
