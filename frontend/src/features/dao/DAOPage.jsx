import React, { useState } from 'react';
import CreateProposal from './CreateProposal';
import ProposalList from './ProposalList';

function DAOPage() {
    const [refreshKey, setRefreshKey] = useState(0);

    const handleProposalCreated = () => {
        setRefreshKey(prevKey => prevKey + 1);
    };

    return (
        <div>
            <h1>PlatformQ DAO</h1>
            <hr />
            <CreateProposal onProposalCreated={handleProposalCreated} />
            <hr />
            <ProposalList key={refreshKey} />
        </div>
    );
}

export default DAOPage; 