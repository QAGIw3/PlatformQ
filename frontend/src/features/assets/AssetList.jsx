import React, { useState, useEffect } from 'react';
import { getDigitalAssets, createPeerReview } from '../../api/client';

function AssetList({ refreshKey }) {
    const [assets, setAssets] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        setLoading(true);
        getDigitalAssets()
            .then(response => {
                setAssets(response.data);
                setLoading(false);
            })
            .catch(error => {
                setError(error);
                setLoading(false);
            });
    }, [refreshKey]);

    const handleAddReview = (assetId, reviewContent) => {
        createPeerReview(assetId, { review_content: reviewContent })
            .then(() => {
                window.location.reload();
            })
            .catch(error => {
                console.error("Failed to add review:", error);
            });
    };

    if (loading) return <p>Loading assets...</p>;
    if (error) return <p>Error loading assets: {error.message}</p>;

    return (
        <div>
            <h2>Digital Assets</h2>
            <ul>
                {assets.map((asset) => (
                    <li key={asset.id}>
                        <h3>{asset.name}</h3>
                        <p>{asset.description}</p>
                        <p><a href={asset.s3_url} target="_blank" rel="noopener noreferrer">Asset Link</a></p>
                        <h4>Peer Reviews</h4>
                        <ul>
                            {asset.reviews.map((review) => (
                                <li key={review.id}>
                                    <p><strong>{review.reviewer_id}:</strong> {review.review_content}</p>
                                </li>
                            ))}
                        </ul>
                        <AddReviewForm assetId={asset.id} onAddReview={handleAddReview} />
                    </li>
                ))}
            </ul>
        </div>
    );
}

function AddReviewForm({ assetId, onAddReview }) {
    const [reviewContent, setReviewContent] = useState('');

    const handleSubmit = (event) => {
        event.preventDefault();
        onAddReview(assetId, reviewContent);
        setReviewContent('');
    };

    return (
        <form onSubmit={handleSubmit}>
            <textarea
                value={reviewContent}
                onChange={(e) => setReviewContent(e.target.value)}
                placeholder="Write a review..."
                required
            />
            <button type="submit">Add Review</button>
        </form>
    );
}

export default AssetList;
