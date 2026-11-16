import { Link } from "react-router-dom";
import ThumbnailRotate from "./components/RotateThumbnail";

export default function Home() {
  return (
    <div className="container mt-3">
        <div>
            <h1>Interactive Video Retrieval</h1>
            <p>Search the video collection using text, visual or ad-hoc search.</p>
        </div>

        <div className="row">
            <div className="col-sm-4">
                <div className="card h-100 overflow-hidden">
                    <div className="card-body">
                        <h5 className="card-title">Text Search (KIS-T)</h5>
                        <p className="card-text">Find videos using descriptions and keywords.</p>
                        <Link to="/text-search"  className="btn btn-primary mt-auto w-100">Go to Search</Link>
                    </div>
                    <div className="card-footer p-0">
                    <ThumbnailRotate images={["/homepage/text_1.jpg", "/homepage/text_2.jpg", "/homepage/text_3.jpg", "/homepage/text_4.jpg", "/homepage/text_5.jpg"]}/></div>
                </div>
            </div>
            <div className="col-sm-4">
                <div className="card h-100 overflow-hidden">
                    <div className="card-body">
                        <h5 className="card-title">Visual Search (KIS-V)</h5>
                        <p className="card-text">Find matching shots using an image or video clip.</p>
                        <Link to="/visual-search"  className="btn btn-primary mt-auto w-100">Go to Search</Link>
                    </div>
                    <div className="card-footer p-0">
                    <ThumbnailRotate images={["/homepage/visual_1.jpg", "/homepage/visual_2.jpg", "/homepage/visual_3.jpg", "/homepage/visual_4.jpg", "/homepage/visual_5.jpg"]}/>
                    </div>
                </div>
            </div>
            <div className="col-sm-4">
                <div className="card h-100 overflow-hidden">
                    <div className="card-body">
                        <h5 className="card-title">Ad-hoc Video Search (AVS)</h5>
                        <p className="card-text">Find relevant shots across multiple videos.</p>
                        <Link to="/avs"  className="btn btn-primary mt-auto w-100">Go to Search</Link>
                    </div>
                    <div className="card-footer p-0">
                    <ThumbnailRotate images={["/homepage/avs_1.jpg", "/homepage/avs_2.jpg", "/homepage/avs_3.jpg", "/homepage/avs_4.jpg", "/homepage/avs_5.jpg"]}/></div>
                </div>
            </div>
        </div>
    </div>
  );
}