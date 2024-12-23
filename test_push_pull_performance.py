from dataclasses import dataclass
from typing import List, Dict, Any, Callable, Tuple
import subprocess
from pprint import pprint
import matplotlib.pyplot as plt
import time
import os
import logging
import numpy as np
from test_functions import * #create_cpu_tests, create_memory_tests, create_noop_tests, create_sleep_tests, TestCase
import psutil
import threading
import argparse


class PerformanceMetrics:
    def __init__(self):
        self.metrics_by_workers = {}  # Dict[num_workers, List[metrics]]
        self.task_counts = []  # List of task counts for x-axis
        
    def add_metrics(self, num_workers: int, task_count: int, metrics: Dict):
        if num_workers not in self.metrics_by_workers:
            self.metrics_by_workers[num_workers] = []
        
        metrics['task_count'] = task_count
        self.metrics_by_workers[num_workers].append(metrics)
        
        if task_count not in self.task_counts:
            self.task_counts.append(task_count)
            self.task_counts.sort()

def plot_comparative_metrics(client, test_case: TestCase):
    metrics_obj = getattr(client, f"{test_case.name}_metrics")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Performance Metrics for {test_case.name}')
    
    colors = plt.cm.rainbow(np.linspace(0, 1, len(metrics_obj.metrics_by_workers)))
    
    for i, (num_workers, metrics_list) in enumerate(metrics_obj.metrics_by_workers.items()):
        color = colors[i]
        task_counts = []
        avg_latencies = []
        success_rates = []
        throughputs = []
        
        for metrics in metrics_list:
            task_counts.append(metrics['task_count'])
            avg_latencies.append(metrics['avg_latency'])
            success_rates.append(metrics['successful_tasks'] / metrics['total_tasks'] * 100)
            # throughputs.append(metrics['successful_tasks'] / metrics['avg_latency'] if metrics['avg_latency'] > 0 else 0)
            avg_latency = metrics['avg_latency'] if metrics['avg_latency'] is not None else 1e-6  # Avoid division by zero
            throughputs.append(metrics['successful_tasks'] / avg_latency)

        label = f'{num_workers} workers'
        ax1.plot(task_counts, avg_latencies, 'o-', label=label, color=color)
        ax2.plot(task_counts, success_rates, 'o-', label=label, color=color)
        ax3.plot(task_counts, throughputs, 'o-', label=label, color=color)
        
        ax4.plot(task_counts, [m['total_tasks'] for m in metrics_list], 'o--', 
                label=f'{label} (total)', color=color, alpha=0.5)
        ax4.plot(task_counts, [m['successful_tasks'] for m in metrics_list], 'o-',
                label=f'{label} (success)', color=color)
    
    ax1.set_xlabel('Total Tasks')
    ax1.set_ylabel('Average Latency (s)')
    ax1.set_title('Latency vs Task Count')
    ax1.legend()
    ax1.grid(True)
    
    ax2.set_xlabel('Total Tasks')
    ax2.set_ylabel('Success Rate (%)')
    ax2.set_title('Success Rate vs Task Count')
    ax2.legend()
    ax2.grid(True)
    
    ax3.set_xlabel('Total Tasks')
    ax3.set_ylabel('Throughput (tasks/s)')
    ax3.set_title('Throughput vs Task Count')
    ax3.legend()
    ax3.grid(True)
    
    ax4.set_xlabel('Total Tasks')
    ax4.set_ylabel('Tasks Count')
    ax4.set_title('Total vs Successful Tasks')
    ax4.legend()
    ax4.grid(True)

    function_comp_plot_dir = f"./testing_plots/comparative/{str(test_case.name)}_comparative"
    os.makedirs(function_comp_plot_dir, exist_ok=True)
    plt.tight_layout()
    plt.savefig(f'{function_comp_plot_dir}/{test_case.name}_comparative_metrics.png')
    plt.close()


def monitor_resources(metrics_dict):
    """Monitor CPU and memory usage during test execution"""
    while threading.main_thread().is_alive():
        metrics_dict['cpu_usage'].append(psutil.cpu_percent(interval=1))
        metrics_dict['memory_usage'].append(psutil.Process().memory_percent())
        time.sleep(0.5)

def run_performance_tests_with_monitoring(client, test_cases, worker_configs, tasks_scaling):
    for test_case in test_cases:
        # Initialize metrics object if it doesn't exist
        if not hasattr(client, f"{test_case.name}_metrics"):
            setattr(client, f"{test_case.name}_metrics", PerformanceMetrics())
        for config in worker_configs:
            for total_tasks in tasks_scaling:
                # Initialize resource metrics
                resource_metrics = {
                    'cpu_usage': [],
                    'memory_usage': []
                }
                
                # Start resource monitoring thread
                monitor_thread = threading.Thread(
                    target=monitor_resources, 
                    args=(resource_metrics,),
                    daemon=True
                )
                monitor_thread.start()
                
                # Run test
                runner = TestRunner(client, config, tasks_per_service=total_tasks)
                metrics = runner.run_single_test(test_case, config[0], total_tasks)
                
                # Add resource utilization to metrics
                metrics['avg_cpu_usage'] = np.mean(resource_metrics['cpu_usage'])
                metrics['avg_memory_usage'] = np.mean(resource_metrics['memory_usage'])
                metrics['throughput'] = metrics['successful_tasks'] / metrics['avg_latency']
                
                # Add to metrics object
                metrics_obj = getattr(client, f"{test_case.name}_metrics")
                metrics_obj.add_metrics(config[0]['num_workers'], total_tasks, metrics)

                print(f"Baseline Metrics:")
                print(f"  Average Latency: {metrics['avg_latency']:.3f}s")
                print(f"  Throughput: {metrics['throughput']:.2f} tasks/s")
                print(f"  Success Rate: {(metrics['successful_tasks']/metrics['total_tasks'])*100:.1f}%")
                print(f"  Avg CPU Usage: {metrics['avg_cpu_usage']:.1f}%")
                print(f"  Avg Memory Usage: {metrics['avg_memory_usage']:.1f}%")
        
        plot_comparative_metrics(client, test_case)

def run_performance_tests(client, test_cases: List[TestCase], worker_configs: List[List[Dict]] = None,
                         tasks_scaling: List[int] = None, model_type="pull") -> None:
    if worker_configs is None:
        worker_configs = [ [{"num_workers": n, "procs_per_worker": [1] * n}] for n in range(1, 6) ]
    
    if tasks_scaling is None:
        tasks_scaling = [1, 2, 5, 10, 25, 50]

    for test_case in test_cases:
        print(f"\n=== Starting test series for {test_case.name} ===")
        
        if not hasattr(client, f"{test_case.name}_metrics"):
            setattr(client, f"{test_case.name}_metrics", PerformanceMetrics())
            
        setattr(client, f"{test_case.name}_test_results", [])
        
        for config in worker_configs:
            for total_tasks in tasks_scaling:
                print(f"\n=== Starting test run with {total_tasks} total tasks ===")
                runner = TestRunner(client, config,model_type=model_type, tasks_per_service=total_tasks)
                runner.plot_num = config[0]['num_workers']
                
                results_list = f"{test_case.name}_test_results"
                # print(f"Results list before test: {len(getattr(client, results_list, []))}")
                
                metrics = runner.run_single_test(test_case, config[0], total_tasks)
                test_results = getattr(client, results_list)
                test_results.append(metrics)
                
                metrics_obj = getattr(client, f"{test_case.name}_metrics")
                metrics_obj.add_metrics(config[0]['num_workers'], total_tasks, metrics)
                
                print(f"Results list after test: {len(getattr(client, results_list, []))}\n")
                print(f"Latest metrics: \n") #{metrics if metrics else 'No results'}")
                pprint(metrics, indent=20) if metrics else print('No results')

                
        plot_comparative_metrics(client, test_case)

class TestRunner:
    def __init__(self, client, worker_configs: List[Dict], 
                #  worker_script: str = "pull_model/pull_worker.py",
                 model_type: str = "pull",
                 dispatcher_url: str = "tcp://127.0.0.1:5556",
                 tasks_per_service: int = 5,
                 plots_dir: str = "./testing_plots"):
        self.client = client
        self.worker_configs = worker_configs
        self.model_type = model_type
        self.worker_script = f"{model_type}_model/{model_type}_worker.py"
        self.dispatcher_url = dispatcher_url
        self.tasks_per_service = tasks_per_service
        self.plots_dir = plots_dir
        self.plot_num = 0
        os.makedirs(plots_dir, exist_ok=True)
        os.makedirs("./logs", exist_ok=True)

    def run_single_test(self, test_case: TestCase, config: Dict, total_tasks: int):
        total_procs = sum(config["procs_per_worker"])
        processes = self.setup_workers(config)
        
        print("\n----------------------------------------------------------------------------\n")
        print(f"\nTesting {total_tasks} tasks with {config['num_workers']} workers ({total_procs} total processors)")
        # print(f"Running {total_tasks} total tasks distributed across workers\n")
        
        try:
            metrics = self.client.run_stress_test(
                test_func=test_case.func,
                num_tasks=total_tasks,
                params=test_case.params,
                concurrent_tasks=total_procs
            )
            print(f"\n{test_case.name.capitalize()} Test Results:")
            pprint(metrics, indent=20); print("\n")
        finally:
            self.cleanup_workers(processes)
            
        return metrics

    def setup_workers(self, config: Dict) -> List[subprocess.Popen]:
        processes = []
        for i in range(config["num_workers"]):
            # log_file = f"./logs/pull_worker_{i+1}.log"
            log_file = f"./logs/{self.model_type}_worker_{i+1}.log"
            with open(log_file, "w") as log:
                command = ["python3", self.worker_script, #str(i+1), 
                          str(config["procs_per_worker"][i]), self.dispatcher_url]
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                processes.append(process)
        time.sleep(0.5)
        return processes

    def cleanup_workers(self, processes: List[subprocess.Popen]):
        for process in processes:
            process.terminate()
        time.sleep(0.5)

def get_concurrency_configs():
    return [
        # Single worker, single process
        [{"num_workers": 1, "procs_per_worker": [1]}],
        
        # Single worker, multiple processes
        [{"num_workers": 1, "procs_per_worker": [2]}],
        [{"num_workers": 1, "procs_per_worker": [4]}],
        
        # Multiple workers, single process each
        [{"num_workers": 2, "procs_per_worker": [1, 1]}]
    ]

def get_baseline_configs():
    base_line_configs = [
        [{"num_workers": 1, "procs_per_worker": [1]}],
        [{"num_workers": 2, "procs_per_worker": [1, 1]}],
        [{"num_workers": 3, "procs_per_worker": [1, 1, 1]}],
        [{"num_workers": 4, "procs_per_worker": [1, 1, 1, 1]}],
        [{"num_workers": 5, "procs_per_worker": [1, 1, 1, 1, 1]}],
        [{"num_workers": 6, "procs_per_worker": [1, 1, 1, 1, 1, 1, 1]}],]
    return base_line_configs

def get_load_test_configs():
   return [
       # Light load - 5 tasks/worker, :3
       [{"num_workers": 1, "procs_per_worker": [1]}, 5],
       [{"num_workers": 2, "procs_per_worker": [1, 1]}, 10],
       [{"num_workers": 4, "procs_per_worker": [1, 1, 1, 1]}, 20],
       
       # Medium load - 20 tasks/worker, 3:6
       [{"num_workers": 1, "procs_per_worker": [1]}, 20],
       [{"num_workers": 2, "procs_per_worker": [1, 1]}, 40],
       [{"num_workers": 4, "procs_per_worker": [1, 1, 1, 1]}, 80],
       
       # Heavy load - 50 tasks/worker, 6:9
       [{"num_workers": 1, "procs_per_worker": [1]}, 50], 
       [{"num_workers": 2, "procs_per_worker": [1, 1]}, 100],
       [{"num_workers": 4, "procs_per_worker": [1, 1, 1, 1]}, 200]
   ]






def main():

    parser = argparse.ArgumentParser(description='Run performance tests')
    parser.add_argument('-m', '--model', type=str, choices=['push', 'pull'], 
                       default='pull', help='Model type (push/pull)')
    args = parser.parse_args()

    from my_faas_client import MPCSFaaSClient
    client = MPCSFaaSClient()
    
    from my_faas_client import MPCSFaaSClient
    client = MPCSFaaSClient()
    model_type = "push"  

    # Normal baseline configs
    worker_configs = get_baseline_configs()[:2]
    tasks_scaling = [1, 2, 5, 10, 15, 20, 50, 100]

    # Use these for load testing
    # load_test_worker_configs = get_load_test_configs()[6:]
    # worker_configs = [[config[0]] for config in load_test_worker_configs]
    # tasks_scaling = [config[1] for config in load_test_worker_configs]


    # Fo weak scaling tests use this config for worker configs and tasks
    # weak_scaling = create_weak_scaling_tests()
    # worker_configs = [[{"num_workers": w, "procs_per_worker": [1] * w}] 
    #                  for w, t in weak_scaling]
    # tasks_scaling = [t for _, t in weak_scaling]
    # print(tasks_scaling)


    
    test_cases = (
                create_noop_tests() 
                  + create_sleep_tests()
                 + create_cpu_tests() 
                 + create_memory_tests()
                + create_io_tests() 
                + create_mixed_tests()
                 )

    run_performance_tests(
        client=client,
        test_cases=test_cases,
        worker_configs=worker_configs,
        tasks_scaling=tasks_scaling,
        model_type=args.model
    )

if __name__ == "__main__":
    main()



