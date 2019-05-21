#!/usr/bin/env bash
set -eu
set +o posix
############################################
# startup script for the flink yarn session
############################################

APPLICATION_DIR="${FLINK_HOME}"
APPLICATION_LOG_DIR="${FLINK_LOG_DIR}"
APPLICATION_NAME="flink-yarn"
PGREP_CMD="FlinkYarnSessionCli"

PID_FILE="${APPLICATION_DIR}/pid"
LOG_FILE="${APPLICATION_LOG_DIR}/${APPLICATION_NAME}_start_stop.log"

RED="\033[01;31m"
BLUE="\033[01;34m"
NORMAL="\033[00m"
YELLOW="\033[33m"
GRAY="\033[01;30m"

COLORLOG="${COLORLOG:-0}"

function log_error() {
  [[ "${COLORLOG}" -eq 1 ]] && \
    { echo -e "${RED}[!]${NORMAL}[$(date '+%Y-%m-%d %H:%M:%S')] $*" 1>&2; } || \
  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [!] $*" 1>&2; }
}
function log_status() {
  [[ "${COLORLOG}" -eq 1 ]] && \
  { echo -e "${BLUE}[>]${NORMAL}[$(date '+%Y-%m-%d %H:%M:%S')] $*"; } || \
  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [>] $*"; }
}
function log_warn() {
  [[ "${COLORLOG}" -eq 1 ]] && \
  { echo -e "${YELLOW}[W]${NORMAL}[$(date '+%Y-%m-%d %H:%M:%S')] $*"; } || \
  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [W] $*"; }
}
function log_progress() {
  [[ "${COLORLOG}" -eq 1 ]] && \
  { echo -e "${GRAY}[ ]${NORMAL}[$(date '+%Y-%m-%d %H:%M:%S')] $*"; } || \
  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ ] $*"; }
}

# create new application log dir if it does not exist
if [[ ! -d "${APPLICATION_LOG_DIR}" ]]; then
  log_warn "${APPLICATION_NAME} log directory ${APPLICATION_LOG_DIR} missing, trying to create it now..."
  mkdir -p "${APPLICATION_LOG_DIR}"
  log_progress "${APPLICATION_NAME} log directory created"
fi

# redirecting stdout and stderr to log file
exec 1> >(tee -a "${LOG_FILE}" 2>&1)

log_status "$0 - executing ${APPLICATION_NAME} kill script"

PID_LIST=$(ps -ef | grep "${PGREP_CMD}" | grep -v " grep " | awk '{print $2}') || true

if [[ -z "${PID_LIST}" ]]; then
  log_status "$0 - no running ${APPLICATION_NAME} process found, stopping now."
  exit 0
fi

log_progress "Found following runnning ${APPLICATION_NAME} processes: ${PID_LIST[*]}"

for PID in "${PID_LIST[@]}"; do
  if [[ -n "${PID}" ]]; then
    log_status "Send TERM signal to process ${PID} to shut it down gracefully."
    kill -s TERM "${PID}"
    while [[ $(ps "${PID}" | grep -c "${PID}") -eq 1 ]]; do
      log_progress "Process ${PID} still running, sleeping 2 seconds..."
      sleep 2
    done
    log_progress "Process with ${PID} terminated."
  fi
done

if [[ -f "${PID_FILE}" ]]; then
  log_progress "Cleaning up PID file \"${PID_FILE}\""
  rm "${PID_FILE}"
fi

log_status "Terminated all ${APPLICATION_NAME} processes and cleaned up PID file."
