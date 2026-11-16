
import { Card, Button } from "react-bootstrap";

export default function VideoCard({ video, onSelect }) {
  return (
    <Card className="h-100 overflow-hidden">
      {video.thumbnail_url && (<Card.Img variant="top" src={video.thumbnail_url} alt={video.title || video.video_id} loading="lazy" style={{height: "180px", objectFit: "cover"}}/>)}

      <Card.Body className="d-flex flex-column">
        <Card.Title className="fs-6">
            {video.title || video.video_id}
        </Card.Title>

        <Card.Text className="small text-muted">
            Video ID: {video.video_id}
        </Card.Text>

        <div className="d-flex justify-content-between small mb-3">
          <span>BM25: {video.score.toFixed(2)}</span>
          <span>{video.shot_count} shots</span>
        </div>

        <Button variant="light" className="mt-auto w-100" onClick={() => onSelect(video.video_id)}> Browse Shots </Button>
      </Card.Body>
    </Card>
  );
}
