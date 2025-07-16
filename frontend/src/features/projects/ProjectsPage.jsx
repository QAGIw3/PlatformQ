import React, { useState } from 'react';
import CreateProject from './CreateProject';
import ProjectList from './ProjectList';

function ProjectsPage() {
    const [refreshKey, setRefreshKey] = useState(0);

    const handleProjectCreated = () => {
        setRefreshKey(prevKey => prevKey + 1);
    };

    return (
        <div>
            <h1>Projects</h1>
            <hr />
            <CreateProject onProjectCreated={handleProjectCreated} />
            <hr />
            <ProjectList key={refreshKey} />
        </div>
    );
}

export default ProjectsPage; 