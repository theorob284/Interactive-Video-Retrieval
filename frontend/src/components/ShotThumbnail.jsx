
import { Card, Button } from "react-bootstrap";

export default function ShotThumbnail({ shot, onSelect, rank, scoreLabel }) {
    const startSeconds = (shot.start_ms / 1000).toFixed(2);
    const endSeconds = (shot.end_ms / 1000).toFixed(2);

    return (
        <Card className="h-100">
        {shot.thumbnail_url && (<Card.Img variant="top" src={shot.thumbnail_url} alt={shot.shot_id}/>)}

        <Card.Body>
            <Card.Title className="fs-6">
                {shot.shot_id}
            </Card.Title>

            <Card.Text className="small">
                {startSeconds}s - {endSeconds}s
            </Card.Text>

            {rank != null && (
                <p className="small text-muted mb-2">
                    Rank #{rank} {scoreLabel && `· ${scoreLabel}`}
                </p>
            )}

            <Button className="mt-auto w-100" variant="light" size="sm" onClick={() => onSelect(shot)}> Select Shot </Button>
        </Card.Body>
        </Card>
    );
}
