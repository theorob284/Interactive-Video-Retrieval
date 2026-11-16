
import { Card, Button } from "react-bootstrap";

export default function ShotThumbnail({ shot, onSelect, rank, scoreLabel, extraAction }) {
    const startSeconds = (shot.start_ms / 1000).toFixed(2);
    const endSeconds = (shot.end_ms / 1000).toFixed(2);

    return (
        <Card className="h-100">
        {shot.thumbnail_url && (<Card.Img variant="top" src={shot.thumbnail_url} alt={shot.shot_id} style={{aspectRatio: "16 / 9", objectFit: "cover",}}/>)}

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

            <div className="d-flex gap-2 mt-auto">
                <Button className="flex-fill text-nowrap" variant="light" size="sm" onClick={() => onSelect(shot)}> Select Shot </Button>
                {extraAction && (
                    <Button variant={extraAction.selected ? "success" : "primary"} size="sm" className="flex-fill" disabled={extraAction.disabled} onClick={() => extraAction.onClick(shot)}>
                        {extraAction.selected ? "Added ✓" : "Add to Submission"}
                    </Button>
                )}
            </div>

        </Card.Body>
        </Card>
    );
}
