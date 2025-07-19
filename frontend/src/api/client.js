import axios from 'axios';

const apiClient = axios.create({
    baseURL: '/api/v1', // This will be proxied by the dev server
    headers: {
        'Content-Type': 'application/json',
    },
});

// Proposals Service
export const getProposals = () => apiClient.get('/governance-service/api/v1/proposals');
export const createProposal = (proposalData) => apiClient.post('/governance-service/api/v1/proposals', proposalData);

// Projects Service
export const getProjects = () => apiClient.get('/projects-service/');
export const createProject = (projectData) => apiClient.post('/projects-service/', projectData);
export const completeMilestone = (projectId, milestoneId) => apiClient.post(`/projects-service/${projectId}/milestones/${milestoneId}/complete`);

// Digital Asset Service
export const getDigitalAssets = () => apiClient.get('/digital-asset-service/digital-assets');
export const createDigitalAsset = (assetData) => apiClient.post('/digital-asset-service/digital-assets', assetData);
export const getAssetLineage = (assetId) => apiClient.get(`/digital-asset-service/digital-assets/${assetId}/lineage`);
export const createPeerReview = (cid, reviewData) => apiClient.post(`/digital-asset-service/digital-assets/${cid}/reviews`, reviewData);
export const listAssetForSale = (assetId, params) => apiClient.post(`/digital-asset-service/digital-assets/${assetId}/list-for-sale`, null, { params });
export const createLicenseOffer = (assetId, terms) => apiClient.post(`/digital-asset-service/digital-assets/${assetId}/create-license-offer`, terms);

// Graph Intelligence Service
export const getTrustScore = (userId) => apiClient.get(`/graph-intelligence-service/trust-score/${userId}`); 