// Save as test-client.js and run with Node
const WebSocket = require('ws');
const ws = new WebSocket('ws://localhost:8080/ws');

ws.on('open', () => {
    // Register Alice
    ws.send(JSON.stringify({
        type: 'register',
        payload: { userId: 'alice' }
    }));
    
    // After 1 second, call Bob
    setTimeout(() => {
        ws.send(JSON.stringify({
            type: 'call',
            to: 'bob', // bob fails but alice succeeds
            payload: { sdp: 'test-offer' }
        }));
    }, 1000);
});

ws.on('message', (data) => {
    console.log('Received:', JSON.parse(data));
});