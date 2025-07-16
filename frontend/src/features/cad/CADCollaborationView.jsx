import React, { useState, useEffect, useRef, useCallback } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls';
import { TransformControls } from 'three/examples/jsm/controls/TransformControls';
import { GLTFLoader } from 'three/examples/jsm/loaders/GLTFLoader';
import { STLLoader } from 'three/examples/jsm/loaders/STLLoader';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader';
import './CADCollaborationView.css';

/**
 * Real-time collaborative CAD/3D editing component
 * Supports multi-user editing with CRDT-based conflict resolution
 */
const CADCollaborationView = ({ assetId, sessionId, userId }) => {
  const mountRef = useRef(null);
  const sceneRef = useRef(null);
  const rendererRef = useRef(null);
  const cameraRef = useRef(null);
  const controlsRef = useRef(null);
  const transformControlsRef = useRef(null);
  const wsRef = useRef(null);
  const animationIdRef = useRef(null);
  
  const [isConnected, setIsConnected] = useState(false);
  const [activeUsers, setActiveUsers] = useState([]);
  const [selectedObject, setSelectedObject] = useState(null);
  const [editMode, setEditMode] = useState('translate'); // translate, rotate, scale
  const [meshStats, setMeshStats] = useState({ vertices: 0, faces: 0 });
  const [pendingOperations, setPendingOperations] = useState([]);
  
  // User cursor positions for collaborative awareness
  const [userCursors, setUserCursors] = useState({});
  
  // Initialize Three.js scene
  useEffect(() => {
    if (!mountRef.current) return;
    
    // Scene setup
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0xf0f0f0);
    scene.fog = new THREE.Fog(0xf0f0f0, 1, 1000);
    sceneRef.current = scene;
    
    // Camera setup
    const camera = new THREE.PerspectiveCamera(
      75,
      mountRef.current.clientWidth / mountRef.current.clientHeight,
      0.1,
      1000
    );
    camera.position.set(5, 5, 5);
    cameraRef.current = camera;
    
    // Renderer setup
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(mountRef.current.clientWidth, mountRef.current.clientHeight);
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    mountRef.current.appendChild(renderer.domElement);
    rendererRef.current = renderer;
    
    // Lighting
    const ambientLight = new THREE.AmbientLight(0x404040);
    scene.add(ambientLight);
    
    const directionalLight = new THREE.DirectionalLight(0xffffff, 1);
    directionalLight.position.set(5, 10, 5);
    directionalLight.castShadow = true;
    directionalLight.shadow.camera.near = 0.1;
    directionalLight.shadow.camera.far = 50;
    directionalLight.shadow.camera.left = -10;
    directionalLight.shadow.camera.right = 10;
    directionalLight.shadow.camera.top = 10;
    directionalLight.shadow.camera.bottom = -10;
    scene.add(directionalLight);
    
    // Grid helper
    const gridHelper = new THREE.GridHelper(20, 20);
    scene.add(gridHelper);
    
    // Axes helper
    const axesHelper = new THREE.AxesHelper(5);
    scene.add(axesHelper);
    
    // Controls
    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controlsRef.current = controls;
    
    // Transform controls for object manipulation
    const transformControls = new TransformControls(camera, renderer.domElement);
    transformControls.addEventListener('change', () => renderer.render(scene, camera));
    transformControls.addEventListener('dragging-changed', (event) => {
      controls.enabled = !event.value;
    });
    scene.add(transformControls);
    transformControlsRef.current = transformControls;
    
    // Animation loop
    const animate = () => {
      animationIdRef.current = requestAnimationFrame(animate);
      controls.update();
      renderer.render(scene, camera);
    };
    animate();
    
    // Handle window resize
    const handleResize = () => {
      if (!mountRef.current) return;
      camera.aspect = mountRef.current.clientWidth / mountRef.current.clientHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(mountRef.current.clientWidth, mountRef.current.clientHeight);
    };
    window.addEventListener('resize', handleResize);
    
    // Cleanup
    return () => {
      window.removeEventListener('resize', handleResize);
      if (animationIdRef.current) {
        cancelAnimationFrame(animationIdRef.current);
      }
      if (mountRef.current && renderer.domElement) {
        mountRef.current.removeChild(renderer.domElement);
      }
      renderer.dispose();
    };
  }, []);
  
  // WebSocket connection for real-time collaboration
  useEffect(() => {
    if (!sessionId) return;
    
    const ws = new WebSocket(
      `ws://localhost:8001/api/v1/ws/cad-sessions/${sessionId}?user_id=${userId}`
    );
    wsRef.current = ws;
    
    ws.onopen = () => {
      console.log('Connected to CAD collaboration session');
      setIsConnected(true);
    };
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      handleWebSocketMessage(data);
    };
    
    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
    
    ws.onclose = () => {
      console.log('Disconnected from CAD collaboration session');
      setIsConnected(false);
    };
    
    return () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, [sessionId, userId]);
  
  // Handle incoming WebSocket messages
  const handleWebSocketMessage = useCallback((data) => {
    switch (data.type) {
      case 'initial_state':
        // Load initial CRDT state
        if (data.crdt_state) {
          loadCRDTState(data.crdt_state);
        }
        setActiveUsers(data.active_users || []);
        break;
        
      case 'user_joined':
        setActiveUsers((prev) => [...prev, data.user_id]);
        showNotification(`${data.user_id} joined the session`);
        break;
        
      case 'user_left':
        setActiveUsers((prev) => prev.filter((id) => id !== data.user_id));
        setUserCursors((prev) => {
          const updated = { ...prev };
          delete updated[data.user_id];
          return updated;
        });
        showNotification(`${data.user_id} left the session`);
        break;
        
      case 'geometry_operation':
        applyRemoteOperation(data.operation, data.from_user);
        break;
        
      case 'cursor_position':
        updateUserCursor(data.user_id, data.position);
        break;
        
      case 'operation_ack':
        handleOperationAcknowledgment(data.operation_id, data.result);
        break;
        
      default:
        console.warn('Unknown message type:', data.type);
    }
  }, []);
  
  // Send geometry operation to server
  const sendGeometryOperation = useCallback((operation) => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      console.error('WebSocket not connected');
      return;
    }
    
    const message = {
      type: 'geometry_operation',
      operation: {
        operation_id: generateOperationId(),
        operation_type: operation.type,
        target_object_ids: operation.targets || [],
        data: operation.data,
        timestamp: Date.now()
      }
    };
    
    wsRef.current.send(JSON.stringify(message));
    setPendingOperations((prev) => [...prev, message.operation]);
  }, []);
  
  // Apply remote operation to local scene
  const applyRemoteOperation = useCallback((operation, fromUser) => {
    if (!sceneRef.current) return;
    
    switch (operation.operation_type) {
      case 'ADD_VERTEX':
      case 'ADD_OBJECT':
        const geometry = createGeometryFromOperation(operation.data);
        const material = new THREE.MeshPhongMaterial({
          color: getUserColor(fromUser),
          transparent: true,
          opacity: 0.8
        });
        const mesh = new THREE.Mesh(geometry, material);
        mesh.userData = {
          id: operation.target_object_ids[0],
          creator: fromUser
        };
        sceneRef.current.add(mesh);
        break;
        
      case 'MOVE_VERTEX':
      case 'TRANSFORM':
        const object = findObjectById(operation.target_object_ids[0]);
        if (object) {
          if (operation.data.position) {
            object.position.set(
              operation.data.position.x,
              operation.data.position.y,
              operation.data.position.z
            );
          }
          if (operation.data.rotation) {
            object.rotation.set(
              operation.data.rotation.x,
              operation.data.rotation.y,
              operation.data.rotation.z
            );
          }
          if (operation.data.scale) {
            object.scale.set(
              operation.data.scale.x,
              operation.data.scale.y,
              operation.data.scale.z
            );
          }
        }
        break;
        
      case 'REMOVE_VERTEX':
      case 'DELETE':
        const toRemove = findObjectById(operation.target_object_ids[0]);
        if (toRemove) {
          sceneRef.current.remove(toRemove);
          toRemove.geometry.dispose();
          toRemove.material.dispose();
        }
        break;
    }
    
    updateMeshStats();
  }, []);
  
  // Load 3D model
  const loadModel = useCallback(async (url, format) => {
    let loader;
    switch (format) {
      case 'gltf':
      case 'glb':
        loader = new GLTFLoader();
        break;
      case 'stl':
        loader = new STLLoader();
        break;
      case 'obj':
        loader = new OBJLoader();
        break;
      default:
        console.error('Unsupported format:', format);
        return;
    }
    
    try {
      const result = await new Promise((resolve, reject) => {
        loader.load(url, resolve, undefined, reject);
      });
      
      let object;
      if (format === 'gltf' || format === 'glb') {
        object = result.scene;
      } else {
        const geometry = result;
        const material = new THREE.MeshPhongMaterial({
          color: 0x808080,
          specular: 0x111111,
          shininess: 200
        });
        object = new THREE.Mesh(geometry, material);
      }
      
      object.traverse((child) => {
        if (child instanceof THREE.Mesh) {
          child.castShadow = true;
          child.receiveShadow = true;
        }
      });
      
      sceneRef.current.add(object);
      
      // Center camera on object
      const box = new THREE.Box3().setFromObject(object);
      const center = box.getCenter(new THREE.Vector3());
      const size = box.getSize(new THREE.Vector3());
      const maxDim = Math.max(size.x, size.y, size.z);
      const fov = cameraRef.current.fov * (Math.PI / 180);
      let cameraZ = Math.abs(maxDim / 2 / Math.tan(fov / 2));
      cameraZ *= 1.5;
      
      cameraRef.current.position.set(center.x, center.y, center.z + cameraZ);
      cameraRef.current.lookAt(center);
      controlsRef.current.target = center;
      
      updateMeshStats();
      
    } catch (error) {
      console.error('Error loading model:', error);
    }
  }, []);
  
  // Handle object selection
  const handleObjectClick = useCallback((event) => {
    if (!rendererRef.current || !sceneRef.current || !cameraRef.current) return;
    
    const rect = rendererRef.current.domElement.getBoundingClientRect();
    const mouse = new THREE.Vector2(
      ((event.clientX - rect.left) / rect.width) * 2 - 1,
      -((event.clientY - rect.top) / rect.height) * 2 + 1
    );
    
    const raycaster = new THREE.Raycaster();
    raycaster.setFromCamera(mouse, cameraRef.current);
    
    const intersects = raycaster.intersectObjects(sceneRef.current.children, true);
    
    if (intersects.length > 0) {
      const object = intersects[0].object;
      selectObject(object);
    } else {
      selectObject(null);
    }
  }, []);
  
  // Select object for transformation
  const selectObject = useCallback((object) => {
    if (transformControlsRef.current) {
      if (object && object.type === 'Mesh') {
        transformControlsRef.current.attach(object);
        setSelectedObject(object);
        
        // Listen for transform changes
        const handleChange = () => {
          sendGeometryOperation({
            type: 'TRANSFORM',
            targets: [object.userData.id || object.uuid],
            data: {
              position: object.position.toArray(),
              rotation: object.rotation.toArray(),
              scale: object.scale.toArray()
            }
          });
        };
        
        transformControlsRef.current.addEventListener('objectChange', handleChange);
        
        // Cleanup
        return () => {
          transformControlsRef.current.removeEventListener('objectChange', handleChange);
        };
      } else {
        transformControlsRef.current.detach();
        setSelectedObject(null);
      }
    }
  }, [sendGeometryOperation]);
  
  // Update mesh statistics
  const updateMeshStats = useCallback(() => {
    if (!sceneRef.current) return;
    
    let totalVertices = 0;
    let totalFaces = 0;
    
    sceneRef.current.traverse((child) => {
      if (child instanceof THREE.Mesh && child.geometry) {
        const geometry = child.geometry;
        if (geometry.attributes.position) {
          totalVertices += geometry.attributes.position.count;
        }
        if (geometry.index) {
          totalFaces += geometry.index.count / 3;
        } else if (geometry.attributes.position) {
          totalFaces += geometry.attributes.position.count / 3;
        }
      }
    });
    
    setMeshStats({ vertices: totalVertices, faces: totalFaces });
  }, []);
  
  // Utility functions
  const generateOperationId = () => {
    return `${userId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  };
  
  const getUserColor = (userId) => {
    const colors = [0xff0000, 0x00ff00, 0x0000ff, 0xffff00, 0xff00ff, 0x00ffff];
    const hash = userId.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
    return colors[hash % colors.length];
  };
  
  const findObjectById = (id) => {
    let found = null;
    sceneRef.current.traverse((child) => {
      if (child.userData.id === id || child.uuid === id) {
        found = child;
      }
    });
    return found;
  };
  
  const createGeometryFromOperation = (data) => {
    // Simple box geometry for demonstration
    return new THREE.BoxGeometry(
      data.size?.x || 1,
      data.size?.y || 1,
      data.size?.z || 1
    );
  };
  
  const loadCRDTState = (crdtStateHex) => {
    // In production, deserialize and apply CRDT state
    console.log('Loading CRDT state...');
  };
  
  const updateUserCursor = (userId, position) => {
    setUserCursors((prev) => ({
      ...prev,
      [userId]: position
    }));
  };
  
  const handleOperationAcknowledgment = (operationId, result) => {
    setPendingOperations((prev) => 
      prev.filter((op) => op.operation_id !== operationId)
    );
    
    if (!result.accepted) {
      console.error('Operation rejected:', operationId);
    }
  };
  
  const showNotification = (message) => {
    // In production, use a proper notification system
    console.log('Notification:', message);
  };
  
  // UI event handlers
  const handleAddCube = () => {
    const geometry = new THREE.BoxGeometry(1, 1, 1);
    const material = new THREE.MeshPhongMaterial({ color: 0x808080 });
    const cube = new THREE.Mesh(geometry, material);
    cube.position.set(0, 0.5, 0);
    cube.castShadow = true;
    cube.receiveShadow = true;
    cube.userData.id = generateOperationId();
    
    sceneRef.current.add(cube);
    
    sendGeometryOperation({
      type: 'ADD_OBJECT',
      targets: [cube.userData.id],
      data: {
        type: 'cube',
        size: { x: 1, y: 1, z: 1 },
        position: { x: 0, y: 0.5, z: 0 }
      }
    });
    
    updateMeshStats();
  };
  
  const handleDelete = () => {
    if (selectedObject) {
      sendGeometryOperation({
        type: 'DELETE',
        targets: [selectedObject.userData.id || selectedObject.uuid],
        data: {}
      });
      
      sceneRef.current.remove(selectedObject);
      selectedObject.geometry.dispose();
      selectedObject.material.dispose();
      selectObject(null);
      updateMeshStats();
    }
  };
  
  const handleModeChange = (mode) => {
    setEditMode(mode);
    if (transformControlsRef.current) {
      transformControlsRef.current.setMode(mode);
    }
  };
  
  const handleOptimizeMesh = async () => {
    if (!selectedObject || !selectedObject.geometry) return;
    
    try {
      // Request mesh optimization
      const response = await fetch('/api/v1/optimize/mesh', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          asset_id: assetId,
          mesh_uri: 's3://meshes/' + assetId,
          optimization_params: {
            type: 'DECIMATE',
            level: 'MEDIUM',
            preserve_features: ['UV_SEAMS', 'MATERIALS']
          }
        })
      });
      
      const result = await response.json();
      console.log('Optimization requested:', result);
      
    } catch (error) {
      console.error('Error requesting optimization:', error);
    }
  };
  
  return (
    <div className="cad-collaboration-container">
      <div className="cad-toolbar">
        <div className="toolbar-section">
          <h3>CAD Collaboration</h3>
          <span className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
            {isConnected ? '● Connected' : '○ Disconnected'}
          </span>
        </div>
        
        <div className="toolbar-section">
          <button onClick={handleAddCube} className="toolbar-btn">
            Add Cube
          </button>
          <button onClick={handleDelete} className="toolbar-btn" disabled={!selectedObject}>
            Delete
          </button>
          <button onClick={handleOptimizeMesh} className="toolbar-btn" disabled={!selectedObject}>
            Optimize
          </button>
        </div>
        
        <div className="toolbar-section">
          <label>Mode:</label>
          <select value={editMode} onChange={(e) => handleModeChange(e.target.value)}>
            <option value="translate">Move</option>
            <option value="rotate">Rotate</option>
            <option value="scale">Scale</option>
          </select>
        </div>
        
        <div className="toolbar-section">
          <span>Vertices: {meshStats.vertices.toLocaleString()}</span>
          <span>Faces: {meshStats.faces.toLocaleString()}</span>
        </div>
      </div>
      
      <div className="cad-main">
        <div className="cad-viewport" ref={mountRef} onClick={handleObjectClick} />
        
        <div className="cad-sidebar">
          <div className="sidebar-section">
            <h4>Active Users ({activeUsers.length})</h4>
            <ul className="user-list">
              {activeUsers.map((user) => (
                <li key={user} className={user === userId ? 'current-user' : ''}>
                  <span className="user-indicator" style={{ backgroundColor: `#${getUserColor(user).toString(16)}` }} />
                  {user} {user === userId && '(You)'}
                </li>
              ))}
            </ul>
          </div>
          
          <div className="sidebar-section">
            <h4>Pending Operations</h4>
            <ul className="operation-list">
              {pendingOperations.slice(-5).map((op) => (
                <li key={op.operation_id}>
                  {op.operation_type} - {new Date(op.timestamp).toLocaleTimeString()}
                </li>
              ))}
            </ul>
          </div>
          
          <div className="sidebar-section">
            <h4>Session Info</h4>
            <p>Session ID: {sessionId}</p>
            <p>Asset ID: {assetId}</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CADCollaborationView; 