import React, { useState, useEffect } from 'react';
import { getProjects, completeMilestone } from '../../api/client';

function ProjectList({ refreshKey }) {
    const [projects, setProjects] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        setLoading(true);
        getProjects()
            .then(response => {
                setProjects(response.data);
                setLoading(false);
            })
            .catch(error => {
                setError(error);
                setLoading(false);
            });
    }, [refreshKey]);

    const handleCompleteMilestone = (projectId, milestoneId) => {
        completeMilestone(projectId, milestoneId)
            .then(() => {
                // Ideally, we would refresh just the one project,
                // but for now, we'll just refresh the whole list.
                // A more sophisticated state management library would help here.
                window.location.reload();
            })
            .catch(error => {
                console.error("Failed to complete milestone:", error);
            });
    };

    if (loading) return <p>Loading projects...</p>;
    if (error) return <p>Error loading projects: {error.message}</p>;

    return (
        <div>
            <h2>Projects</h2>
            <ul>
                {projects.map((project) => (
                    <li key={project.id}>
                        <h3>{project.title}</h3>
                        <p>{project.description}</p>
                        <ul>
                            {project.milestones.map((milestone) => (
                                <li key={milestone.id}>
                                    {milestone.title} - {milestone.completed ? 'Completed' : 'Pending'}
                                    {!milestone.completed && (
                                        <button onClick={() => handleCompleteMilestone(project.id, milestone.id)}>
                                            Complete
                                        </button>
                                    )}
                                </li>
                            ))}
                        </ul>
                    </li>
                ))}
            </ul>
        </div>
    );
}

export default ProjectList; 