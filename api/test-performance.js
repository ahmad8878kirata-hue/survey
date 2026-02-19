const axios = require('axios');

async function testPagination() {
    const baseUrl = 'http://localhost:3000/api/surveys';
    console.log('--- Testing Pagination API ---');

    try {
        // Note: This test assumes the server is running.
        // If it's not, we might need to start it or mock the response.
        // For this environment, I'll try to just check the response structure if I can.

        const response = await axios.get(`${baseUrl}?page=1&limit=5`);
        console.log('Status:', response.status);
        console.log('Pagination Data:', response.data.pagination);

        if (response.data.pagination && response.data.pagination.page === 1) {
            console.log('✅ Success: Pagination data returned correctly.');
        } else {
            console.log('❌ Failure: Pagination data missing or incorrect.');
        }

        if (Array.isArray(response.data.managers) && Array.isArray(response.data.workers)) {
            console.log('✅ Success: Survey records returned as arrays.');
        }

    } catch (error) {
        if (error.code === 'ECONNREFUSED') {
            console.log('ℹ️ Server is not running. Verification needs to be done manually or after starting the server.');
        } else {
            console.error('❌ Error testing API:', error.message);
        }
    }
}

testPagination();
