import os
import sys
import time
from filelock import FileLock
import psutil
import daemon
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import PID_LOCK_FILE, PID_FILE


def get_process_memory(pid):
    """Get the memory usage of the current process
    Returns:
        float: The memory usage in MB, otherwise -1 if the process
        does not exist.
    """
    try:
        process = psutil.Process(pid)
        return process.memory_info().rss / 1024 / 1024
    except psutil.NoSuchProcess:
        return -1

def add_process_to_pid_list(pid, name):
    """Add the current process to the pid file.
    First we read the pid file to get the list of processes. Then we clean the
    list by removing any processes that are no longer running. Finally we add
    the current process to the list and write the list back to the pid file.
    Args:
        pid (int): The process id
        name (str): The process name
    Note: If pid is < 0 or name is empty, the process is not added to the list.
    This effectively cleans the list of any dead processes."""

    pid_info = []
    try:
        # Get a lock on the pid file
        with FileLock(PID_LOCK_FILE, timeout=10):
            # Read in the pid,name tuple list from the file
            # use with to open config['PID_FILE'] only if the file exists
            if os.path.exists(PID_FILE):
                with open(PID_FILE, 'r') as f:
                    lines = f.read().splitlines()
                    for line in lines:
                        parts = line.split()
                        pid_info.append((int(parts[0]), parts[1]))
        # Remove any entries where the process does not exist
        pid_info = [(pid, name) for pid, name in pid_info if psutil.pid_exists(pid)]

        # Add the new entry. If pid < 0 or name is empty, do not add the entry.
        # This is for cleaning the list of dead processes.
        if pid >= 0:
            pid_info.append((pid, name))

        # Write the list back to the file
        with open(PID_FILE, 'w') as f:
            for pid, name in pid_info:
                f.write(f"{pid} {name}\n")
    except Exception as e:
        # Ignore the error if the pid file does not exist. Return an empty list.
        pass

def add_this_process_to_pid_list(name):
    """Add the current process to the pid file.
    Args:
        name (str): The process name
    """
    add_process_to_pid_list(os.getpid(), name)    

def remove_process_from_list(process_name):
    """Remove a process from the list based on the process name
    Args:
        name (str): The process name
    
    """
    pid_info = []
    try:
        # Get a lock on the pid file
        with FileLock(PID_LOCK_FILE, timeout=10):
            # Read in the pid,name tuple list from the file
            # use with to open config['PID_FILE'] only if the file exists
            if os.path.exists(PID_FILE):
                with open(PID_FILE, 'r') as f:
                    lines = f.read().splitlines()
                    for line in lines:
                        parts = line.split()
                        pid_info.append((int(parts[0]), parts[1]))
        # Remove any entries where the process does not exist
        print(process_name)
        print(f"pid_info: {pid_info}")
        pid_info = [(pid, name) for pid, name in pid_info if psutil.pid_exists(pid) and name != process_name]

        print(f"pid_info: {pid_info}")

        # Write the list back to the file
        with open(PID_FILE, 'w') as f:
            for pid, name in pid_info:
                f.write(f"{pid} {name}\n")
    except Exception as e:
        # Ignore the error if the pid file does not exist. Return an empty list.
        pass

# Read all process ids from the pid file
def get_pids():
    """Get the process ids from the pid file
    Returns:
        list: A list of tuples containing the process id, name and 
            memory usage in MB
    Note: The pid file is a text file with one line per process. Each line
    contains the process id and the name of the process. The name is used
    to identify the process. The memory usage is obtained from the process
    id.
    """
    pids_info = []
    try:
        with FileLock(PID_LOCK_FILE, timeout=10):
            # use with to open config['PID_FILE'] only if the file exists
            if os.path.exists(PID_FILE):
                with open(PID_FILE, 'r') as f:
                    lines = f.read().splitlines()
                    for line in lines:
                        parts = line.split()
                        pid = int(parts[0])
                        if psutil.pid_exists(pid):
                            name = parts[1]
                            pids_info.append((pid, name, get_process_memory(pid)))
        
    except Exception as e:
        # Ignore the error if the pid file does not exist. Return an empty list.
        pass

    return pids_info 

def clean():
    """Clean the pid file by removing any dead processes. Processes are not killed."""
    add_process_to_pid_list(-1, None)

def kill_process(name):
    """Kill a process based on the process name
    Args:
        name (str): The process name. If name is None, kill ALL processes.
    """
    MAX_SECS_TO_WAIT_FOR_KILL = 10
    secs_count = 0
    found = False
    pids_info = get_pids()
    for pid, pname, _ in pids_info:
        if name is None or pname == name:
            found = True
            os.kill(pid, 9)
            print(f"Killed process {pid}, {pname}")
            # The process does not die immediately, wait till it is dead so the following clean will work.
            while psutil.pid_exists(pid):
                time.sleep(0.1)
                secs_count += 0.1
                if secs_count > MAX_SECS_TO_WAIT_FOR_KILL:
                    print(f"Unable to kill process {pid} {pname}")
                    break

    if not found:
        print(f"No process found with name {name}")

    # Now clean any dead processes from the pid file
    clean()

def daemonize(name, task, kill_existing=True):
    """Daemonize the current process.
    Args:
        name (str): The process name. This can be any name to identify the task,
            but it should be unique. Does not necessarily have to be the name of
            the python module.
        task (function): The task to run in the daemon process
        kill_existing (bool): Kill any existing processes/daemon with the same name before creating this new one.
    """
    if kill_existing:
        kill_process(name)
    with daemon.DaemonContext():
        add_this_process_to_pid_list(name)
        task()

if __name__ == "__main__":
    # Get command line arguments
    if len(sys.argv) == 1:
        print("Usage: python processes.py <command> <name>")
        print("Commands:")
        print("  clean - clean the pid file, no processes are killed")
        print("  kill <name> - kill a process by name, if name is not specified, all processes are killed. Note: the pid.txt is updated after the process is killed")
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "clean":
            print("Cleaning pid file")
            #kill_process(None)
            clean()
        elif command == "kill":
            if len(sys.argv) > 2:
                name = sys.argv[2]
            else:
                name = None
            kill_process(name)
        elif command == "list":
            pids_info = get_pids()
            if len(pids_info) == 0:
                print("No processes are in the list of PIDs")
            for pid, name, mem in pids_info:
                print(f"{pid}, {name}, {mem:.2f} MB")
        else:
            print(f"Unknown command: {command}")