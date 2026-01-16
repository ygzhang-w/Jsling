# Jsling - Remote Job Submission System

Jsling is a command-line tool for submitting and managing jobs on remote Slurm clusters. It simplifies cross-cluster job submission, status monitoring, and file synchronization.

## Installation

**Requirements:** Python 3.11+ and uv package manager

```bash
# Clone and install
git clone <repository_url>
cd Jsling
uv venv
source .venv/bin/activate
uv pip install -e .

# Initialize
uv run jsling init
```

## Usage

### 1. Manage Workers

```bash
# Add a worker (Slurm cluster)
uv run jsling worker add --config worker.yml

# List workers
uv run jsling worker list

# Test connection
uv run jsling worker test <worker-id>
```

### 2. Start Background Daemon

```bash
# Start runner daemon (for automatic status updates)
uv run jsling runner start

# Check status
uv run jsling runner status
```

### 3. Submit and Monitor Jobs

```bash
# Submit a job
uv run jsub job_config.yml

# Check job status
uv run jqueue

# Cancel a job
uv run jcancel <job-id>

# Sync files
uv run jsync <job-id>
```

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=jsling
```



## License

TBD
