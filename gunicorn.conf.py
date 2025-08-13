# Gunicorn configuration file
# This ensures sync workers are used instead of eventlet

# Worker class - explicitly use sync workers
worker_class = "sync"

# Number of worker processes
workers = 2

# Timeout for workers
timeout = 120

# Bind address
bind = "0.0.0.0:8000"

# Preload app for better performance
preload_app = True

# Maximum requests before worker restart
max_requests = 1000
max_requests_jitter = 100

# Worker timeout
worker_timeout = 120

# Keep alive
keepalive = 2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "ather-crm"

# Disable eventlet completely
disable_redirect_input_to_caller = True
