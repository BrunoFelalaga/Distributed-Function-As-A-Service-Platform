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
        self.metrics_by_workers = {}
        self.task_counts = []
        
    def add_metrics(self, num_workers: int, task_count: int, metrics: Dict):
        if num_workers not in self.metrics_by_workers:
            self.metrics_by_workers[num_workers] = []
        metrics['task_count'] = task_count
        self.metrics_by_workers[num_workers].append(metrics)
        if task_count not in self.task_counts:
            self.task_counts.append(task_count)
            self.task_counts.sort()

class LocalDispatcherTestRunner:
    def __init__(self, client, num_processes: int = 4):
        self.client = client
        self.num_processes = num_processes
        self.output_queue = queue.Queue()
        os.makedirs("./testing_plots/comparative", exist_ok=True)
        os.makedirs("./logs", exist_ok=True)
        
    def start_dispatcher(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dispatcher_path = 'local_dispatcher_model/local_dispatcher.py'
        process = subprocess.Popen(
            ['python3', dispatcher_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        def read_output():
            while True:
                line = process.stdout.readline()
                print(f"STDOUT: {line.strip()}")
                
                err = process.stderr.readline()
                if err:
                    print(f"STDERR: {err.strip()}")

                if not line and not err and process.poll() is not None:
                    break
                    
        threading.Thread(target=read_output, daemon=True).start()
        return process

    def run_single_test(self, test_case: TestCase, config: Dict, total_tasks: int):
        dispatcher_process = self.start_dispatcher()
        time.sleep(1)  # Allow dispatcher to initialize
        
        resource_metrics = {'cpu_usage': [], 'memory_usage': []}
        monitor_thread = threading.Thread(
            target=self.monitor_resources,
            args=(resource_metrics,),
            daemon=True
        )
        monitor_thread.start()

        try:
            metrics = self.client.run_stress_test(
                test_func=test_case.func,
                num_tasks=total_tasks,
                params=test_case.params,
                concurrent_tasks=self.num_processes,
                test_type=test_case.name
            )
            
            if metrics:
                metrics.update({
                    'avg_cpu_usage': np.mean(resource_metrics['cpu_usage']) if resource_metrics['cpu_usage'] else 0,
                    'avg_memory_usage': np.mean(resource_metrics['memory_usage']) if resource_metrics['memory_usage'] else 0,
                    'throughput': metrics['successful_tasks'] / (metrics.get('avg_latency', 0.001) or 0.001)
                })
            
            return metrics
            
        finally:
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
    if not metrics_obj.metrics_by_workers:
        return
        
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Performance Metrics for {test_case.name}')
    
    colors = plt.cm.rainbow(np.linspace(0, 1, len(metrics_obj.metrics_by_workers)))
    
    for i, (num_workers, metrics_list) in enumerate(metrics_obj.metrics_by_workers.items()):
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
            label = f'{num_workers} workers'
            ax1.plot(task_counts, avg_latencies, 'o-', label=label, color=color)
            ax2.plot(task_counts, success_rates, 'o-', label=label, color=color)
            ax3.plot(task_counts, throughputs, 'o-', label=label, color=color)
            ax4.plot(task_counts, [m.get('total_tasks', 0) for m in metrics_list], 'o--', 
                    label=f'{label} (total)', color=color, alpha=0.5)
            ax4.plot(task_counts, [m.get('successful_tasks', 0) for m in metrics_list], 'o-',
                    label=f'{label} (success)', color=color)
    
    for ax in [ax1, ax2, ax3, ax4]:
        ax.legend()
        ax.grid(True)
    
    ax1.set_xlabel('Total Tasks'); ax1.set_ylabel('Average Latency (s)')
    ax2.set_xlabel('Total Tasks'); ax2.set_ylabel('Success Rate (%)')
    ax3.set_xlabel('Total Tasks'); ax3.set_ylabel('Throughput (tasks/s)')
    ax4.set_xlabel('Total Tasks'); ax4.set_ylabel('Tasks Count')
    
    plot_dir = f"./testing_plots/comparative/{test_case.name}_comparative"
    os.makedirs(plot_dir, exist_ok=True)
    plt.tight_layout()
    plt.savefig(f'{plot_dir}/{test_case.name}_comparative_metrics.png')
    plt.close()

def run_performance_tests(client, test_cases: List[TestCase], worker_configs=None, tasks_scaling=None):
    if worker_configs is None:
        worker_configs = [[{"num_workers": n, "procs_per_worker": [1] * n}] for n in range(1, 6)]
    if tasks_scaling is None:
        tasks_scaling = [1, 2, 5]

    for test_case in test_cases:
        if not hasattr(client, f"{test_case.name}_metrics"):
            setattr(client, f"{test_case.name}_metrics", PerformanceMetrics())
        
        for config in worker_configs:
            for total_tasks in tasks_scaling:
                runner = LocalDispatcherTestRunner(client, config[0]["num_workers"])
                metrics = runner.run_single_test(test_case, config[0], total_tasks)
                
                if metrics:
                    metrics_obj = getattr(client, f"{test_case.name}_metrics")
                    metrics_obj.add_metrics(config[0]['num_workers'], total_tasks, metrics)
                
        plot_comparative_metrics(client, test_case)

def main():
    client = MPCSFaaSClient()
    os.system("redis-cli flushall")
    
    test_cases = create_noop_tests() + create_sleep_tests() + create_io_tests() + create_mixed_tests()
    tasks_scaling = [1, 2, 5, 10]
    worker_configs = [[{"num_workers": n, "procs_per_worker": [1] * n}] for n in range(1, 3)]
    
    try:
        run_performance_tests(
            client=client,
            test_cases=test_cases,
            worker_configs=worker_configs,
            tasks_scaling=tasks_scaling
        )
    finally:
        os.system("pkill -f local_dispatcher")

if __name__ == "__main__":
    main()
