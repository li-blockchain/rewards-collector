// Rate limiting configuration for beaconcha.in API
// 5 requests/second, 20 requests/minute, 30000/day, 30000/month
class RateLimiter {
  constructor() {
    this.requestQueue = [];
    this.secondRequests = [];
    this.minuteRequests = [];
    this.processing = false;
  }

  async makeRequest(requestFn) {
    return new Promise((resolve, reject) => {
      this.requestQueue.push({ requestFn, resolve, reject });
      this.processQueue();
    });
  }

  async processQueue() {
    if (this.processing || this.requestQueue.length === 0) return;
    
    this.processing = true;
    
    while (this.requestQueue.length > 0) {
      const now = Date.now();
      
      // Clean up old requests
      this.secondRequests = this.secondRequests.filter(time => now - time < 1000);
      this.minuteRequests = this.minuteRequests.filter(time => now - time < 60000);
      
      // Check if we can make a request
      if (this.secondRequests.length >= 4 || this.minuteRequests.length >= 18) {
        // Wait before trying again - use conservative limits (4/second, 18/minute)
        const waitTime = this.secondRequests.length >= 4 ? 1000 : 60000;
        await new Promise(resolve => setTimeout(resolve, waitTime));
        continue;
      }
      
      const { requestFn, resolve, reject } = this.requestQueue.shift();
      
      try {
        this.secondRequests.push(now);
        this.minuteRequests.push(now);
        
        const result = await requestFn();
        resolve(result);
        
        // Small delay between requests to be extra safe
        await new Promise(resolve => setTimeout(resolve, 300));
        
      } catch (error) {
        reject(error);
      }
    }
    
    this.processing = false;
  }
}

module.exports = RateLimiter;