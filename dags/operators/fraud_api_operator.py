from airflow.sdk import BaseOperator
import requests
import time
from datetime import timedelta
from typing import Dict, Any, Optional




class FraudAPIOperator(BaseOperator):
    """
    Operator to call fraud detection APIs with built-in retry and error handling.
    
    :param endpoint: API endpoint to call
    :param method: HTTP method (GET, POST, etc.)
    :param payload: Optional request payload for POST/PUT
    :param timeout: Request timeout in seconds
    :param max_retries: Maximum number of retry attempts
    :param retry_delay: Delay between retries in seconds
    
    Example usage:
        fetch_data = FraudAPIOperator(
            task_id='fetch_transactions',
            endpoint='https://jsonplaceholder.typicode.com/posts',
            method='GET',
            timeout=10,
            max_retries=3
        )
    """
    
    # Template fields allow Jinja templating
    template_fields = ('endpoint', 'payload')
    
    # UI color for this operator in Graph view
    ui_color = '#ff6b6b'
    
    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        payload: Optional[Dict] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.payload = payload
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the API call with retry logic
        
        TODO: Implement the following:
        1. Log the API call details
        2. Attempt API call with retry logic (self.max_retries times)
        3. Handle different HTTP methods (GET, POST)
        4. Parse response as JSON
        5. Log success/failure
        6. Return parsed response
        
        Hints:
        - Use self.log.info() for logging
        - Use requests.get() or requests.post()
        - Catch requests.exceptions.RequestException
        - time.sleep(self.retry_delay) between retries
        - response.json() to parse response
        """
        
        self.log.info(f"🌐 Calling API: {self.method} {self.endpoint}")
        self.log.info(f"   Timeout: {self.timeout}s, Max retries: {self.max_retries}")
        
        # TODO: Implement retry logic
        for attempt in range(1, self.max_retries + 1):
            try:
                self.log.info(f"📡 Attempt {attempt}/{self.max_retries}")
                if self.method == 'GET':
                    response = requests.get(self.endpoint, timeout = self.timeout)
                elif self.method == 'POST':
                    response = requests.post(self.endpoint, json=self.payload, timeout = self.timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method: {self.method}")
                
                response.raise_for_status()
                data = response.json()

                self.log.info(f"✅ API call succeeded: {response.status_code}")
                self.log.info(f"   Response size: {len(str(data))} bytes")
                
                return {
                    'status': 'success',
                    'data': data,
                    'attempts': attempt
                }
                # TODO: Check response status
                # TODO: Parse JSON
                # TODO: Log success
                # TODO: Return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                self.log.warning(f"⏱️ Timeout on attempt {attempt}: {str(e)}")
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                self.log.warning(f"❌ HTTP error on attempt {attempt}: {str(e)}")
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                self.log.warning(f"🔌 Network error on attempt {attempt}: {str(e)}")
            
            # If not the last attempt, wait before retrying
            if attempt < self.max_retries:
                self.log.info(f"⏳ Waiting {self.retry_delay_seconds}s before retry...")
                time.sleep(self.retry_delay_seconds)  
                

        
        # If we get here, all retries failed
        error_msg = f"Failed to call API after {self.max_retries} attempts. Last error: {str(last_exception)}"
        self.log.error(f"💥 {error_msg}")
        raise Exception(error_msg)