import { useState } from "react";

function BM25() {
    const [query, setQuery] = useState('')
    const [results, setResults] = useState([])
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState('')

    async function handleSearch(event) {
        event.preventDefault()

        if (!query.trim()) return

        setLoading(true)
        setError('')
        setResults([])

        try {
        const response = await fetch(
            `/api/search/text/?q=${encodeURIComponent(query)}`
        )

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

        <div className="results">
            {results.map((video, index) => (
            <div key={video.video_id} className="result-card">
                <h3>{index + 1}. {video.title}</h3>
                <p>Video ID: {video.video_id}</p>
                <p>BM25 Score: {video.score.toFixed(2)}</p>
            </div>
            ))}
        </div>
        </div>
    )
}

export default BM25