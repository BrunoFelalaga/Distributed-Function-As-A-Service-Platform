import sys
import os
import subprocess
import argparse
import time


if __name__ == "__main__":
    # Argument parser for handling mode, port, and workers
    parser = argparse.ArgumentParser(description='Task Dispatcher')
    parser.add_argument('-m', '--mode', required=True, 
                        choices=['local', 'pull', 'push'],
                        help='Mode of operation')
    parser.add_argument('-p', '--port', type=int, default=5556,
                        help='Port number for pull/push modes')  
    parser.add_argument('-w', '--workers', type=int, default=4,
                        help='Number of worker processes for local mode')

    args = parser.parse_args()

    # Map dispatcher modes to their respective scripts
    dispatcher_scripts = {
        'local': 'local_dispatcher_model/local_dispatcher.py',
        'pull': 'pull_model/pull_dispatcher.py',
        'push': 'push_model/push_dispatcher.py'
    }

    try:
        # Determine the script to run based on the mode
        dispatcher_script = dispatcher_scripts.get(args.mode)

        # Construct the subprocess command
        command = [
            "python3", dispatcher_script
        ]
        
        # Add arguments for specific modes
        if args.mode == 'local':
            command += ['-w', str(args.workers)]
        elif args.mode in ['pull', 'push']:
            command += ['-p', str(args.port)]

        # Start the subprocess
        print(f"Starting {args.mode} dispatcher with command: {' '.join(command)}")
        process = subprocess.Popen(command)

        # Wait for the process to complete or keep it alive indefinitely
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nTerminating the dispatcher...")
        process.terminate()
        process.wait()
    except Exception as e:
        print(f"An error occurred: {e}")
