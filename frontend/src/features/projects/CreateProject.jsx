import React, { useState } from 'react';
import { createProject } from '../../api/client';

function CreateProject({ onProjectCreated }) {
    const [title, setTitle] = useState('');
    const [description, setDescription] = useState('');

    const handleSubmit = (event) => {
        event.preventDefault();
        createProject({ title, description, milestones: [] })
            .then(() => {
                setTitle('');
                setDescription('');
                if (onProjectCreated) {
                    onProjectCreated();
                }
            })
            .catch(error => {
                console.error("Failed to create project:", error);
            });
    };

    return (
        <div>
            <h2>Create New Project</h2>
            <form onSubmit={handleSubmit}>
                <div>
                    <label>Title:</label>
                    <input
                        type="text"
                        value={title}
                        onChange={(e) => setTitle(e.target.value)}
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
                <button type="submit">Create Project</button>
            </form>
        </div>
    );
}

export default CreateProject; 