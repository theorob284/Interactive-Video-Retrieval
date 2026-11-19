import { useState, useRef, useEffect } from "react";
import VideoCard from "./components/VideoCard";
import ShotBrowser from "./components/ShotBrowser";
import VideoPlayer from "./components/VideoPlayer";

function BM25() {
    const [query, setQuery] = useState('')
    const [results, setResults] = useState([])
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState('')
    const [selectedVideoId, setSelectedVideoId] = useState(null);
    const [selectedShot, setSelectedShot] = useState(null);
    const [suggestedShotId, setSuggestedShotId] = useState(null);
    const shotBrowserRef = useRef(null);
    const videoPlayerRef = useRef(null);

    useEffect(() => {
        if (selectedVideoId && shotBrowserRef.current) {
            shotBrowserRef.current.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
        }
    }, [selectedVideoId]);

    useEffect(() => {
        if (selectedShot && videoPlayerRef.current) {
            videoPlayerRef.current.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
        }
    }, [selectedShot]);

    async function handleSearch(event) {
        event.preventDefault()

        if (!query.trim()) return

        setLoading(true)
        setError('')
        setResults([])
        setSelectedVideoId(null);
        setSelectedShot(null);
        setSuggestedShotId(null);

        try {
            const response = await fetch(`/api/search/text/?q=${encodeURIComponent(query)}`)

            if (!response.ok) {
                throw new Error('Search request failed')
            }

            const data = await response.json()
            setResults(data.results)
        } catch (err) {
            setError(err.message)
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="container mt-3">
            <h1>Text Search (KIS-T) - Video Retrieval</h1>
            <p>Search the V3C video collection using BM25.</p>

            <form className="d-flex gap-2 search-form" onSubmit={handleSearch}>
                <input className="form-control mb-2" type="text" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Describe the video you're looking for..."/>

                <button className="btn btn-primary" type="submit" disabled={loading}>
                    {loading ? 'Searching...' : 'Search'}
                </button>
            </form>

            {error && <p>{error}</p>}
 
            {selectedVideoId && (
                <div ref={shotBrowserRef} className="mt-4 shot-browser-section">

                    {selectedShot && (
                        <div ref={videoPlayerRef} className="video-player-section">
                            {selectedShot.shot_id === suggestedShotId && (
                                <div className="alert alert-success mb-2">
                                    <strong>Suggested match:</strong>{" "}{selectedShot.shot_id} {" · "} {(selectedShot.start_ms / 1000).toFixed(2)}s{" - "} {(selectedShot.end_ms / 1000).toFixed(2)}s
                                </div>
                            )}

                            <VideoPlayer videoId={selectedVideoId} shot={selectedShot}/>
                        </div>
                    )}
                    <ShotBrowser videoId={selectedVideoId} suggestedShotId={suggestedShotId} onClose={() => {setSelectedVideoId(null); setSelectedShot(null); setSuggestedShotId(null);}} onSelectShot={setSelectedShot}/>

                </div>
            )}

            <div className="row g-3 mt-2 mb-2">
                {results.map((video) => (
                    <div className="col-12 col-md-6 col-lg-3" key={video.video_id}>
                        <VideoCard video={video} onSelect={(videoId) => {setSelectedVideoId(videoId); setSelectedShot(null); setSuggestedShotId(video.suggested_shot_id);}}/>
                    </div>
                ))}
            </div>

        </div>
    )
}

export default BM25