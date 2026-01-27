#!/bin/bash
#
# Install/uninstall systemd units for LIBC Rewards Backfiller
#
# Usage:
#   sudo ./install.sh [--uninstall]
#
# Before running, edit the .service file to match your paths:
#   - User/Group
#   - WorkingDirectory
#   - EnvironmentFile
#   - ExecStart (venv python path)
#   - ReadWritePaths

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="rewards-backfiller"
SYSTEMD_DIR="/etc/systemd/system"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

install() {
    log_info "Installing ${SERVICE_NAME} systemd units..."

    # Check if files exist
    if [[ ! -f "${SCRIPT_DIR}/${SERVICE_NAME}.service" ]]; then
        log_error "Service file not found: ${SCRIPT_DIR}/${SERVICE_NAME}.service"
        exit 1
    fi

    if [[ ! -f "${SCRIPT_DIR}/${SERVICE_NAME}.timer" ]]; then
        log_error "Timer file not found: ${SCRIPT_DIR}/${SERVICE_NAME}.timer"
        exit 1
    fi

    # Copy unit files
    log_info "Copying unit files to ${SYSTEMD_DIR}..."
    cp "${SCRIPT_DIR}/${SERVICE_NAME}.service" "${SYSTEMD_DIR}/"
    cp "${SCRIPT_DIR}/${SERVICE_NAME}.timer" "${SYSTEMD_DIR}/"

    # Set permissions
    chmod 644 "${SYSTEMD_DIR}/${SERVICE_NAME}.service"
    chmod 644 "${SYSTEMD_DIR}/${SERVICE_NAME}.timer"

    # Reload systemd
    log_info "Reloading systemd daemon..."
    systemctl daemon-reload

    # Enable timer
    log_info "Enabling ${SERVICE_NAME}.timer..."
    systemctl enable "${SERVICE_NAME}.timer"

    # Start timer
    log_info "Starting ${SERVICE_NAME}.timer..."
    systemctl start "${SERVICE_NAME}.timer"

    log_info "Installation complete!"
    echo ""
    log_info "Useful commands:"
    echo "  systemctl status ${SERVICE_NAME}.timer    # Check timer status"
    echo "  systemctl list-timers                     # List all timers"
    echo "  journalctl -u ${SERVICE_NAME}.service     # View service logs"
    echo "  systemctl start ${SERVICE_NAME}.service   # Run manually now"
    echo ""

    # Show timer status
    log_info "Current timer status:"
    systemctl status "${SERVICE_NAME}.timer" --no-pager || true
}

uninstall() {
    log_info "Uninstalling ${SERVICE_NAME} systemd units..."

    # Stop timer if running
    if systemctl is-active --quiet "${SERVICE_NAME}.timer" 2>/dev/null; then
        log_info "Stopping ${SERVICE_NAME}.timer..."
        systemctl stop "${SERVICE_NAME}.timer"
    fi

    # Disable timer
    if systemctl is-enabled --quiet "${SERVICE_NAME}.timer" 2>/dev/null; then
        log_info "Disabling ${SERVICE_NAME}.timer..."
        systemctl disable "${SERVICE_NAME}.timer"
    fi

    # Remove unit files
    if [[ -f "${SYSTEMD_DIR}/${SERVICE_NAME}.service" ]]; then
        log_info "Removing ${SERVICE_NAME}.service..."
        rm "${SYSTEMD_DIR}/${SERVICE_NAME}.service"
    fi

    if [[ -f "${SYSTEMD_DIR}/${SERVICE_NAME}.timer" ]]; then
        log_info "Removing ${SERVICE_NAME}.timer..."
        rm "${SYSTEMD_DIR}/${SERVICE_NAME}.timer"
    fi

    # Reload systemd
    log_info "Reloading systemd daemon..."
    systemctl daemon-reload

    log_info "Uninstallation complete!"
}

# Main
check_root

case "${1:-}" in
    --uninstall|-u)
        uninstall
        ;;
    --help|-h)
        echo "Usage: sudo $0 [--uninstall]"
        echo ""
        echo "Options:"
        echo "  --uninstall, -u    Remove systemd units"
        echo "  --help, -h         Show this help message"
        echo ""
        echo "Before installing, edit rewards-backfiller.service to match your setup."
        ;;
    *)
        install
        ;;
esac
