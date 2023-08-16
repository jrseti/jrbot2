import os
import sys
import time
from filelock import FileLock
import psutil
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
    """Clean the pid file by removing any dead processes"""
    add_process_to_pid_list(-1, None)