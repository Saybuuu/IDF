MAX_RETRIES = 5
INITIAL_DELAY = 1  
BACKOFF_FACTOR = 2
RETRY_STATUSES = {429, 500, 502, 503, 504}

URL = 'http://api.open-notify.org/astros.json'