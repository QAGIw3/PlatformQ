import axios from 'axios';

const apiClient = axios.create({
    baseURL: '/api/v1', // This will be proxied by the dev server
    headers: {
        'Content-Type': 'application/json',
    },
});

// Proposals Service
export const getProposals = () => apiClient.get('/proposals-service/proposals');
export const createProposal = (proposalData) => apiClient.post('/proposals-service/proposals', proposalData);

// Projects Service
export const getProjects = () => apiClient.get('/projects-service/');
export const createProject = (projectData) => apiClient.post('/projects-service/', projectData);
export const completeMilestone = (projectId, milestoneId) => apiClient.post(`/projects-service/${projectId}/milestones/${milestoneId}/complete`);

// Digital Asset Service
export const getDigitalAssets = () => apiClient.get('/digital-asset-service/digital-assets');
export const createDigitalAsset = (assetData) => apiClient.post('/digital-asset-service/digital-assets', assetData);
export const createPeerReview = (assetId, reviewData) => apiClient.post(`/digital-asset-service/digital-assets/${assetId}/reviews`, reviewData);

// Graph Intelligence Service
export const getTrustScore = (userId) => apiClient.get(`/graph-intelligence-service/trust-score/${userId}`); 