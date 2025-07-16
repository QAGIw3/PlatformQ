import React, { useState } from 'react';
import { createDigitalAsset } from '../../api/client';

function CreateAsset({ onAssetCreated }) {
    const [name, setName] = useState('');
    const [description, setDescription] = useState('');
    const [s3Url, setS3Url] = useState('');

    const handleSubmit = (event) => {
        event.preventDefault();
        createDigitalAsset({ name, description, s3_url: s3Url })
            .then(() => {
                setName('');
                setDescription('');
                setS3Url('');
                if (onAssetCreated) {
                    onAssetCreated();
                }
            })
            .catch(error => {
                console.error("Failed to create asset:", error);
            });
    };

    return (
        <div>
            <h2>Create New Digital Asset</h2>
            <form onSubmit={handleSubmit}>
                <div>
                    <label>Name:</label>
                    <input
                        type="text"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        required
                    />
                </div>
                <div>
                    <label>Description:</label>
                    <textarea
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                        required
                    />
                </div>
                <div>
                    <label>S3 URL:</label>
                    <input
                        type="text"
                        value={s3Url}
                        onChange={(e) => setS3Url(e.target.value)}
                        required
                    />
                </div>
                <button type="submit">Create Asset</button>
            </form>
        </div>
    );
}

export default CreateAsset; 