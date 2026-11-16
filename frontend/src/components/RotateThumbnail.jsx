
import { useEffect, useState } from "react";
import "../css/RotateThumbnail.css";

export default function ThumbnailRotate({ images }) {
    const [index, setIndex] = useState(0);

    useEffect(() => {
        if (images.length <= 1) return;

        const interval = setInterval(() => {setIndex((current) => (current + 1) % images.length);}, 3000);

        return () => clearInterval(interval);
    }, [images.length]);

    return (
        <div className="thumbnail-rotate">
            {images.map((image, i) => (<img key={image} src={image} alt="V3C video frame" className={`thumbnail-slide ${i === index ? "active" : ""}`}loading="lazy"/>))}
        </div>
    );
}
