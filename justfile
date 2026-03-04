# Justfile for DICOM-Listener project

# Default recipe to display help
default:
    @just --list

# Start Docker Compose services
up:
    docker compose up -d --build

# Stop Docker Compose services
down:
    docker compose down

# Restart Docker Compose services
restart:
    docker compose restart

# View logs from all services
logs:
    docker compose logs -f

# View logs from specific service (e.g., just logs-service dicom-sorter)
logs-service service:
    docker compose logs -f {{service}}

# Check status of all services
status:
    docker compose ps

# Send DICOM files to the listener (default: /home/thendriks/data)
send folder="/home/thendriks/data" host="localhost" port="104":
    uv run --with pynetdicom --with pydicom test.py {{folder}} --host {{host}} --port {{port}}

# Send DICOM files with debug logging
send-debug folder="/home/thendriks/data" host="localhost" port="104":
    uv run --with pynetdicom --with pydicom test.py {{folder}} --host {{host}} --port {{port}} --debug

# Install Python dependencies
install:
    uv sync

# Sync Python dependencies
sync:
    uv sync

# Run tests
test:
    uv run pytest

# Run pre-commit hooks on all files
lint:
    uv run pre-commit run --all-files

# Check RabbitMQ queue
check-queue:
    @echo "RabbitMQ Management UI: http://localhost:15672"
    @echo "Credentials: user/password"

# Clean up Docker volumes and rebuild from scratch
clean-build:
    docker compose down -v
    docker compose up -d --build

# View dicom-sorter logs
dicom-logs:
    docker compose logs -f dicom-sorter

# View postgres logs
postgres-logs:
    docker compose logs -f postgres

# View rabbitmq logs
rabbitmq-logs:
    docker compose logs -f rabbitmq

# Execute psql in postgres container
psql:
    docker compose exec postgres psql -U postgres -d postgres

# Stash changes and checkout branch
checkout branch:
    git stash
    git checkout {{branch}}

# Checkout digione branch and restart services
digione:
    git stash
    git checkout digione
    docker compose down
    docker compose up -d --build
