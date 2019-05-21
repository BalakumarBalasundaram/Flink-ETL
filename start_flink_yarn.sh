#!/usr/bin/env bash
set -eu
set +o posix
############################################
# startup script for the flink yarn session
############################################

NO_OF_TASKMANAGERS="1"
TASKMANAGER_MEMORY="1024"
JOBMANAGER_MEMORY="768"
QUEUE_NAME="default"

APPLICATION_DIR="${FLINK_HOME}"
APPLICATION_LOG_DIR="${FLINK_LOG_DIR}"
APPLICATION_NAME="flink-yarn"
PGREP_CMD="yarn-session.sh"
LOG4J2_CONFIG="-Dlog4j.configurationFile=${APPLICATION_DIR}/conf/flink-yarn_log4j2.xml"
STARTUP_CMD="${FLINK_HOME}/bin/yarn-session.sh -n ${NO_OF_TASKMANAGERS} -s 2 -qu ${QUEUE_NAME} -tm ${TASKMANAGER_MEMORY} -jm ${JOBMANAGER_MEMORY}"

PID_FILE="${APPLICATION_DIR}/pid_flink_yarn"
LOG_FILE="${APPLICATION_LOG_DIR}/${APPLICATION_NAME}_start_stop.log"
TIMEOUT=120

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

NOW=$(date '+%Y-%m-%d-%H-%M-%S')

# create new application log dir if it does not exist
if [[ ! -d "${APPLICATION_LOG_DIR}" ]]; then
  log_warn "${APPLICATION_NAME} log directory ${APPLICATION_LOG_DIR} missing, trying to create it now..."
  mkdir -p "${APPLICATION_LOG_DIR}"
  log_progress "${APPLICATION_NAME} log directory created"
fi

# redirecting stdout and stderr to log file
exec 1> >(tee -a "${LOG_FILE}" 2>&1)
log_status "$0 - Executing ${APPLICATION_NAME} startup script"

# reading PID from file and write
log_progress "checks if ${APPLICATION_NAME} is already running"
USER=$(whoami)
if [ -f "${PID_FILE}" ]; then
  PID=$(head -1 "${PID_FILE}")
else
  PID=
  log_progress "No PID file found"
fi

# obtaining pid from possible background processes
PID_B=$(ps -ef | grep "${PGREP_CMD}" | grep -v " grep " | awk '{print $2}') || { log_progress "No running process found"; }

log_progress "sync check: PS: ${PID_B} vs. File: ${PID} "

if [[ "${PID}" != "${PID_B}" ]]; then
  log_warn "PID file \"${PID_FILE}\" not in sync"

  if [[ -n "${PID_B}" ]]; then
    log_status "Adding currently running process id ${PID_B} to PID file"
    echo "${PID_B}" > "${PID_FILE}"
    log_warn "No new process started!"
    exit 0
  else
    log_progress "No running process found but PID file contains ${PID}. Cleaning it up..."
    rm "${PID_FILE}"
  fi
fi
if [[ -n "${PID_B}" ]]; then
  log_status "${APPLICATION_NAME} already running with process id ${PID_B}"
  log_warn "No new process started!"
  exit 0
fi

log_progress "passed single ${APPLICATION_NAME} instance check"

log_status "$0 - ${NOW} starting up ${APPLICATION_NAME} service"
export JVM_ARGS="${JVM_ARGS:-} ${LOG4J2_CONFIG}"
nohup ${STARTUP_CMD} >"${APPLICATION_LOG_DIR}/${APPLICATION_NAME}.out.log" 2>&1 &
NEW_PID="$!"
echo "${NEW_PID}" > "${PID_FILE}"

#checking if process has successfully started up
log_status "Waiting for JobManager to be ready..."
timeout --signal SIGINT ${TIMEOUT} grep -q -m 1 "Flink JobManager is now running" <(tail -f "${APPLICATION_LOG_DIR}/${APPLICATION_NAME}.out.log") || {
  log_error "JobManager still not ready after ${TIMEOUT} seconds - please investigate."
  exit 124
}

log_status "JobManager ready, ${APPLICATION_NAME} successfully started."
