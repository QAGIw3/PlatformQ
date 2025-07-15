import React, { useState, useEffect, useRef } from 'react';
import { useAuth } from '../contexts/AuthContext';

const apiClient = {
  getAgentStates: async (simId, token) => {
    const res = await fetch(`/api/v1/simulations/${simId}/state`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    if (!res.ok) throw new Error("Failed to fetch agent states");
    return res.json();
  }
};

function SimulationViewer({ simulationId }) {
  const [agents, setAgents] = useState([]);
  const canvasRef = useRef(null);
  const { token } = useAuth();

  useEffect(() => {
    if (!token || !simulationId) return;
    const intervalId = setInterval(() => {
      apiClient.getAgentStates(simulationId, token)
        .then(setAgents)
        .catch(console.error);
    }, 2000); // Poll every 2 seconds
    return () => clearInterval(intervalId);
  }, [simulationId, token]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !agents.length) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#000033';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    agents.forEach(agent => {
      ctx.fillStyle = agent.agent_type === 'consumer' ? '#33cc33' : '#cc3333';
      ctx.beginPath();
      ctx.arc(agent.position_x, agent.position_y, 5, 0, 2 * Math.PI);
      ctx.fill();
    });
  }, [agents]);

  return (
    <div>
      <h3>Live Simulation Viewer: {simulationId}</h3>
      <canvas ref={canvasRef} width="800" height="600" style={{ border: '1px solid black' }} />
    </div>
  );
}

export default SimulationViewer; 