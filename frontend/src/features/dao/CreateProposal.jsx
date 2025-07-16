import React, { useState } from 'react';
import { createProposal } from '../../api/client';

function CreateProposal({ onProposalCreated }) {
    const [title, setTitle] = useState('');
    const [description, setDescription] = useState('');

    const handleSubmit = (event) => {
        event.preventDefault();
        createProposal({ title, description, targets: [], values: [], calldatas: [] })
            .then(() => {
                setTitle('');
                setDescription('');
                if (onProposalCreated) {
                    onProposalCreated();
                }
            })
            .catch(error => {
                console.error("Failed to create proposal:", error);
                // TODO: Show an error message to the user
            });
    };

    return (
        <div>
            <h2>Create New Proposal</h2>
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
                <button type="submit">Create Proposal</button>
            </form>
        </div>
    );
}

export default CreateProposal; 