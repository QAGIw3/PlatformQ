import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

// --- Configuration ---
const VUS = 50; // Number of concurrent virtual users
const DURATION = '30s';
const BASE_URL = 'http://<KONG_IP_ADDRESS>'; // IMPORTANT: Replace with your Kong LoadBalancer IP

// --- Test Data ---
// Generate a unique email for each virtual user
const data = new SharedArray('users', function () {
    const users = [];
    for (let i = 0; i < VUS; i++) {
        users.push({
            email: `testuser_${__VU}_${i}@platformq.com`,
            full_name: `Test User ${__VU}`,
        });
    }
    return users;
});

// --- Test Scenarios ---
export const options = {
  stages: [
    { duration: '15s', target: VUS }, // Ramp-up
    { duration: DURATION, target: VUS },
    { duration: '5s', target: 0 },   // Ramp-down
  ],
};

export default function () {
  const user = data[__VU - 1];

  // 1. Create a new user
  const createUserRes = http.post(
    `${BASE_URL}/auth/api/v1/users/`,
    JSON.stringify({ email: user.email, full_name: user.full_name }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  check(createUserRes, { 'user created': (r) => r.status === 201 });

  sleep(1);

  // 2. Request a passwordless login token
  const requestTokenRes = http.post(
    `${BASE_URL}/auth/api/v1/login/passwordless`,
    JSON.stringify({ email: user.email }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  check(requestTokenRes, { 'login token requested': (r) => r.status === 200 });
  const tempToken = requestTokenRes.json('temp_token');
  
  sleep(1);

  // 3. Exchange the temporary token for a real JWT
  if (tempToken) {
    const exchangeTokenRes = http.post(
      `${BASE_URL}/auth/api/v1/token/passwordless`,
      JSON.stringify({ email: user.email, temp_token: tempToken }),
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(exchangeTokenRes, { 'token exchanged': (r) => r.status === 200 });
    const accessToken = exchangeTokenRes.json('access_token');
    
    // 4. Use the JWT to access a protected route
    if (accessToken) {
        const meRes = http.get(`${BASE_URL}/auth/api/v1/users/me`, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        check(meRes, { 'fetched user profile': (r) => r.status === 200 });
    }
  }
} 