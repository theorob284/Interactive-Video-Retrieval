import { useState } from "react";
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

    async function handleSearch(event) {
        event.preventDefault()

        if (!query.trim()) return

        setLoading(true)
        setError('')
        setResults([])
        setSelectedVideoId(null);
        setSelectedShot(null);

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
        <div className="container">
            <h1>Interactive Video Retrieval</h1>
            <p>Search the V3C video collection using BM25.</p>

            <form onSubmit={handleSearch}>
                <input type="text" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Describe the video you're looking for..."/>

                <button type="submit" disabled={loading}>
                {loading ? 'Searching...' : 'Search'}
                </button>
            </form>

            {error && <p>{error}</p>}

            {selectedVideoId && (
                <>
                    {selectedShot && (<VideoPlayer videoId={selectedVideoId} shot={selectedShot}/>)}
                    <ShotBrowser videoId={selectedVideoId} onClose={() => {setSelectedVideoId(null); setSelectedShot(null);}} onSelectShot={setSelectedShot}/>
                </>
            )}

            <div className="results">
                {results.map((video) => (<VideoCard key={video.video_id} video={video} onSelect={(videoId) => {setSelectedVideoId(videoId); setSelectedShot(null)}}/>))}
            </div>

        </div>
    )
}

export default BM25