import os
import sys
import time
from filelock import FileLock
import psutil
import daemon
import argparse
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import PID_LOCK_FILE, PID_FILE


class Processes:
    """Class containing static methods for managing processes in the jrbot environment."""

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def add_this_process_to_pid_list(name):
        """Add the current process to the pid file.
        Args:
            name (str): The process name
        """
        Processes.add_process_to_pid_list(os.getpid(), name)    

    @staticmethod
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

    @staticmethod
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
                                pids_info.append((pid, name, Processes.get_process_memory(pid)))
            
        except Exception as e:
            # Ignore the error if the pid file does not exist. Return an empty list.
            pass

        return pids_info 

    @staticmethod
    def clean():
        """Clean the pid file by removing any dead processes. Processes are not killed."""
        Processes.add_process_to_pid_list(-1, None)

    @staticmethod
    def kill_process(name, silent=False):
        """Kill a process based on the process name
        Args:
            name (str): The process name. If name is None, kill ALL processes.
            silent (bool): If True, do not print any messages. Default is False.
        """
        MAX_SECS_TO_WAIT_FOR_KILL = 10
        secs_count = 0
        found = False
        pids_info = Processes.get_pids()
        for pid, pname, _ in pids_info:
            if name is None or pname == name:
                found = True
                os.kill(pid, 9)
                if not silent:
                    print(f"Killed process {pid}, {pname}")
                # The process does not die immediately, wait till it is dead so the following clean will work.
                while psutil.pid_exists(pid):
                    time.sleep(0.1)
                    secs_count += 0.1
                    if secs_count > MAX_SECS_TO_WAIT_FOR_KILL:
                        if not silent:
                            print(f"Unable to kill process {pid} {pname}")
                        break

        if not found:
            if not silent:
                print(f"No process found with name {name}")

        # Now clean any dead processes from the pid file
        Processes.clean()

    @staticmethod
    def daemonize(task, task_args, name, working_dir, kill_existing):
        """Daemonize the current process.
        Args:
            task (function): The task to run in the daemon process
            task_args (dict): The arguments to pass to the task
            name (str): The process name. This can be any name to identify the task,
                but it should be unique. Does not necessarily have to be the name of
                the python module.
            working_dir: The working directory for the daemon process. If None, the
                current working directory is used, which may be /. This is because
                the working directory is changed to / when the process is daemonized.
            kill_existing (bool): Kill any existing processes/daemon with the same 
                name before creating this new one.
        """
        if kill_existing:
            Processes.kill_process(name, True)
        with daemon.DaemonContext():
            Processes.add_this_process_to_pid_list(name)
            if working_dir is not None:
                os.chdir(working_dir)
            task(task_args)

if __name__ == "__main__":

    """Command line interface for managing processes in the jrbot environment."""

    parser = argparse.ArgumentParser(prog=os.path.basename(__file__), 
                                     description='Helper for \
                                     jrbot processes. Allows simple viewing of \
                                     all the running programs/daemons and also killing them.')
    
    parser.add_argument('-c', '--clean', action='store_true', help='Clean the \
                        pid file, no processes are killed')
    parser.add_argument('-k', '--kill', nargs='?', const='all', help='Kill a \
                        process by name, if name is not specified, all \
                        processes are killed. Note: the pid.txt is updated after the process is killed')
    parser.add_argument('-l', '--list', action='store_true', help='List all \
                        processes in the pid file')
    args = parser.parse_args()

    if args.clean:
        print('Cleaning pid file')
        Processes.clean()
    elif args.kill is not None:
        if args.kill == 'all':
            Processes.kill_process(None)
        else:
            Processes.kill_process(args.kill)
    elif args.list:
        pids_info = Processes.get_pids()
        if len(pids_info) == 0:
            print('No processes are in the list of PIDs')
        for pid, name, mem in pids_info:
            print(f'{pid}, {name}, {mem:.2f} MB')
    else:
        parser.print_help()
