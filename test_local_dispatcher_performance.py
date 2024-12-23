
import sys
import os
import time
import logging
import threading
import subprocess
from typing import List, Dict
import matplotlib.pyplot as plt
import numpy as np
import psutil
from test_functions import *
from my_faas_client import MPCSFaaSClient
import queue
import select

# Set up logging config - good practice to track what's happening
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(
    filename='./logs/local_dispatcher.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

def log_and_print(message, level=logging.INFO):
    logging.log(level, message)
    if level in (logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL):
        print(message)

class PerformanceMetrics:
    def __init__(self):
        # Track metrics by process count instead of worker count
        self.metrics_by_processes = {}
        self.task_counts = []
        
    def add_metrics(self, num_processes: int, task_count: int, metrics: Dict):
        # Store metrics organized by process count
        if num_processes not in self.metrics_by_processes:
            self.metrics_by_processes[num_processes] = []
        metrics['task_count'] = task_count
        self.metrics_by_processes[num_processes].append(metrics)
        if task_count not in self.task_counts:
            self.task_counts.append(task_count)
            self.task_counts.sort()

class LocalDispatcherTestRunner:
    def __init__(self, client, num_processes: int = 4):
        self.client = client
        # Single worker but variable processes
        self.num_processes = num_processes
        self.output_queue = queue.Queue()
        
        # Make sure our output directories exist
        os.makedirs("./testing_plots/comparative", exist_ok=True)
        os.makedirs("./logs", exist_ok=True)
        
    def start_dispatcher(self):
        # Start the local dispatcher with configured number of processes
        dispatcher_path = 'local_dispatcher_model/local_dispatcher.py'
        process = subprocess.Popen(
            ['python3', dispatcher_path, str(self.num_processes)],  # Pass process count
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        # Helper function to read dispatcher output
        def read_output():
            while True:
                line = process.stdout.readline()
                if line:
                    print(f"Dispatcher: {line.strip()}")
                
                err = process.stderr.readline()
                if err:
                    print(f"Dispatcher Error: {err.strip()}")

                if not line and not err and process.poll() is not None:
                    break
                    
        threading.Thread(target=read_output, daemon=True).start()
        return process

    def run_single_test(self, test_case: TestCase, total_tasks: int):
        # Start dispatcher with our process count
        dispatcher_process = self.start_dispatcher()
        time.sleep(1)  # Give it a moment to get ready
        
        # Track system resource usage during the test
        resource_metrics = {'cpu_usage': [], 'memory_usage': []}
        monitor_thread = threading.Thread(
            target=self.monitor_resources,
            args=(resource_metrics,),
            daemon=True
        )
        monitor_thread.start()

        try:
            # Run the actual test
            metrics = self.client.run_stress_test(
                test_func=test_case.func,
                num_tasks=total_tasks,
                params=test_case.params,
                concurrent_tasks=self.num_processes,
                test_type=test_case.name
            )
            
            if metrics:
                # Add resource usage to metrics
                metrics.update({
                    'avg_cpu_usage': np.mean(resource_metrics['cpu_usage']) if resource_metrics['cpu_usage'] else 0,
                    'avg_memory_usage': np.mean(resource_metrics['memory_usage']) if resource_metrics['memory_usage'] else 0,
                    'throughput': metrics['successful_tasks'] / (metrics.get('avg_latency', 0.001) or 0.001)
                })
            
            return metrics
            
        finally:
            # Clean up
            dispatcher_process.terminate()
            dispatcher_process.wait()

    def monitor_resources(self, metrics_dict):
        while threading.main_thread().is_alive():
            try:
                metrics_dict['cpu_usage'].append(psutil.cpu_percent(interval=1))
                metrics_dict['memory_usage'].append(psutil.Process().memory_percent())
            except Exception as e:
                log_and_print(f"Error monitoring resources: {str(e)}", logging.ERROR)
            time.sleep(0.5)

def plot_comparative_metrics(client, test_case: TestCase):
    metrics_obj = getattr(client, f"{test_case.name}_metrics")
    if not metrics_obj.metrics_by_processes:  # Changed from metrics_by_workers
        return
        
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Performance Metrics for {test_case.name} - Local Dispatcher')
    
    colors = plt.cm.rainbow(np.linspace(0, 1, len(metrics_obj.metrics_by_processes)))
    
    # Plot metrics for different process counts
    for i, (num_processes, metrics_list) in enumerate(metrics_obj.metrics_by_processes.items()):
        if not metrics_list:
            continue
            
        color = colors[i]
        task_counts = []
        avg_latencies = []
        success_rates = []
        throughputs = []
        
        for metrics in metrics_list:
            if metrics is None:
                continue
            task_counts.append(metrics['task_count'])
            avg_latencies.append(metrics.get('avg_latency', 0))
            success_rates.append((metrics.get('successful_tasks', 0) / metrics.get('total_tasks', 1)) * 100)
            throughputs.append(metrics.get('throughput', 0))

        if task_counts:
            label = f'{num_processes} processes'  # Changed from workers to processes
            ax1.plot(task_counts, avg_latencies, 'o-', label=label, color=color)
            ax2.plot(task_counts, success_rates, 'o-', label=label, color=color)
            ax3.plot(task_counts, throughputs, 'o-', label=label, color=color)
            ax4.plot(task_counts, [m.get('total_tasks', 0) for m in metrics_list], 'o--', 
                    label=f'{label} (total)', color=color, alpha=0.5)
            ax4.plot(task_counts, [m.get('successful_tasks', 0) for m in metrics_list], 'o-',
                    label=f'{label} (success)', color=color)
    
    # Pretty up the plots
    for ax in [ax1, ax2, ax3, ax4]:
        ax.legend()
        ax.grid(True)
    
    ax1.set_xlabel('Total Tasks'); ax1.set_ylabel('Average Latency (s)')
    ax2.set_xlabel('Total Tasks'); ax2.set_ylabel('Success Rate (%)')
    ax3.set_xlabel('Total Tasks'); ax3.set_ylabel('Throughput (tasks/s)')
    ax4.set_xlabel('Total Tasks'); ax4.set_ylabel('Tasks Count')
    
    # Save our beautiful plots
    plot_dir = f"./testing_plots/comparative/{test_case.name}_comparative"
    os.makedirs(plot_dir, exist_ok=True)
    plt.tight_layout()
    plt.savefig(f'{plot_dir}/{test_case.name}_local_dispatcher_metrics.png')
    plt.close()

def run_performance_tests(client, test_cases: List[TestCase], tasks_scaling=None):
    if tasks_scaling is None:
        tasks_scaling = [1, 2, 5, 10]

    # Test with increasing process counts (1-4) on single worker
    process_counts = [1, 2, 3, 4]

    for test_case in test_cases:
        # Initialize metrics tracking
        if not hasattr(client, f"{test_case.name}_metrics"):
            setattr(client, f"{test_case.name}_metrics", PerformanceMetrics())
        
        # Run tests for each process count and task scale
        for num_processes in process_counts:
            for total_tasks in tasks_scaling:
                print(f"\nTesting {test_case.name} with {num_processes} processes and {total_tasks} tasks")
                runner = LocalDispatcherTestRunner(client, num_processes)
                metrics = runner.run_single_test(test_case, total_tasks)
                
                if metrics:
                    metrics_obj = getattr(client, f"{test_case.name}_metrics")
                    metrics_obj.add_metrics(num_processes, total_tasks, metrics)
                
        plot_comparative_metrics(client, test_case)

def main():
    # Start fresh
    client = MPCSFaaSClient()
    os.system("redis-cli flushall")
    
    # Pick our test cases
    test_cases = (
        create_noop_tests() +  # Quick baseline tests
        create_sleep_tests() +  # Test scheduling behavior
        create_cpu_tests() +    # Test CPU-bound tasks
        create_memory_tests()   # Test memory-bound tasks
    )
    
    # Define how many tasks to try
    tasks_scaling = [1, 2, 5, 10, 20]
    
    try:
        run_performance_tests(
            client=client,
            test_cases=test_cases,
            tasks_scaling=tasks_scaling
        )
    finally:
        # Clean up after ourselves
        os.system("pkill -f local_dispatcher")

if __name__ == "__main__":
    main()
