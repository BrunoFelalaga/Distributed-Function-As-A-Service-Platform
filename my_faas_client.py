# faas_client.py
import requests
import uuid
import time
import dill
import codecs
import statistics
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
import matplotlib.pyplot as plt
import numpy as np

class MPCSFaaSClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        # Initialize client with default base URL
        self.base_url = base_url
        self.registered_functions = {}
        self.tasks = {}
        # Separate lists for different test results
        self.sleep_test_results = []
        self.add_test_results = []
        self.multiply_test_results = []
        self.noop_test_results = []
        
    def serialize(self, obj) -> str:
         # Serialize an object to a base64-encoded string
        return codecs.encode(dill.dumps(obj), "base64").decode()
        
    def deserialize(self, obj: str):
        return dill.loads(codecs.decode(obj.encode(), "base64"))
    
    def register_function(self, func, name: str = None) -> uuid.UUID:
        """Register a function with the FaaS service"""
        if name is None:
            name = func.__name__  # Use function's name if not provided
            
        payload = self.serialize(func)  # Serialize function
        response = requests.post(
            f"{self.base_url}/register_function",
            json={"name": name, "payload": payload}
        )
        response.raise_for_status()
        function_id = response.json()["function_id"] # Extract function ID
        self.registered_functions[function_id] = name
        return function_id
    
    def execute_function(self, function_id: uuid.UUID, *args, **kwargs) -> uuid.UUID:
        """Execute a registered function with given arguments"""
        payload = self.serialize((args, kwargs)) # Serialize arguments
        response = requests.post(
            f"{self.base_url}/execute_function",
            json={"function_id": str(function_id), "payload": payload}
        )
        response.raise_for_status()
        task_id = response.json()["task_id"] # Extract task ID
        self.tasks[task_id] = {
            "function_id": function_id,
            "start_time": time.time(),
            "status": "QUEUED"
        }
        return task_id
    
    def get_task_status(self, task_id: uuid.UUID) -> str:
        """Get status of a task"""
        response = requests.get(f"{self.base_url}/status/{task_id}")
        response.raise_for_status()
        status = response.json()["status"]
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = status  # Update status in tasks
        return status
    
    def get_task_result(self, task_id: uuid.UUID, timeout: float = None) -> Any:
        """Get result of a task, optionally waiting until timeout"""
        start_time = time.time()
        while timeout is None or time.time() - start_time < timeout:
            response = requests.get(f"{self.base_url}/result/{task_id}")
            if response.status_code == 404: # Task not ready
                time.sleep(0.1)
                continue
                
            response.raise_for_status()
            result = response.json() # Extract result
            
            if result["status"] in ["COMPLETED", "FAILED"]:
                if task_id in self.tasks:
                    self.tasks[task_id].update({
                        "end_time": time.time(),
                        "status": result["status"]
                    })
                # Deserialize result if successful
                return self.deserialize(result["result"]) if result["status"] == "COMPLETED" else result["result"]
                
            time.sleep(0.1)
            
        raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")
    
    def run_stress_test(self, test_func, num_tasks: int, params=None, concurrent_tasks: int = 10, test_type="generic", num_workers: int = 1) -> Dict:
        """Run stress test with given function and collect performance metrics"""
        print(f"Current results count: {len(getattr(self, f'{test_type}_test_results', []))}")
        
        # Initialize results list if it doesn't exist
        results_list = f"{test_type}_test_results"
        if not hasattr(self, results_list):
            setattr(self, results_list, [])
            
        # Register test function
        func_id = self.register_function(test_func)
        
        # Execute tasks concurrently
        task_ids = []
        with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
            futures = [
                executor.submit(self.execute_function, func_id, *(params if params else ()))
                for _ in range(num_tasks)
            ]
            task_ids = [f.result() for f in futures]
        
        # Collect results and metrics
        results = []
        latencies = []
        failures = 0
        
        for task_id in task_ids:
            try:
                result = self.get_task_result(task_id, timeout=30)
                task_data = self.tasks[task_id]
                latency = task_data["end_time"] - task_data["start_time"]
                latencies.append(latency)
                if task_data["status"] == "FAILED":
                    failures += 1
                results.append((task_id, result))
            except TimeoutError:
                failures += 1
        
        metrics = {
            "total_tasks": num_tasks,
            "successful_tasks": num_tasks - failures,
            "failed_tasks": failures,
            "avg_latency": statistics.mean(latencies) if latencies else None,
            "median_latency": statistics.median(latencies) if latencies else None,
            "min_latency": min(latencies) if latencies else None,
            "max_latency": max(latencies) if latencies else None,
            "latencies": latencies,
            "concurrent_tasks": concurrent_tasks,
            "num_workers": num_workers
        }
        
        # Before appending results
        before_count = len(getattr(self, results_list))
        getattr(self, results_list).append(metrics)
        after_count = len(getattr(self, results_list))
        print(f"Results count before: {before_count}, after: {after_count}")
            
        return metrics
    
    def run_stress_test000(self, test_func, num_tasks: int, params=None, concurrent_tasks: int = 10, test_type="generic", num_workers: int = 1) -> Dict:
        """Run stress test with given function and collect performance metrics"""
        # Register test function
        func_id = self.register_function(test_func)
        
        # Execute tasks concurrently
        task_ids = []
        with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
            futures = [
                executor.submit(self.execute_function, func_id, *(params if params else ()))
                for _ in range(num_tasks)
            ]
            task_ids = [f.result() for f in futures]
        
        # Collect results and metrics
        results = []
        latencies = []
        failures = 0
        
        for task_id in task_ids:
            try:
                result = self.get_task_result(task_id, timeout=30)
                task_data = self.tasks[task_id]
                latency = task_data["end_time"] - task_data["start_time"]
                latencies.append(latency)
                if task_data["status"] == "FAILED":
                    failures += 1
                results.append((task_id, result))
            except TimeoutError:
                failures += 1
        
        metrics = {
            "total_tasks": num_tasks,
            "successful_tasks": num_tasks - failures,
            "failed_tasks": failures,
            "avg_latency": statistics.mean(latencies) if latencies else None,
            "median_latency": statistics.median(latencies) if latencies else None,
            "min_latency": min(latencies) if latencies else None,
            "max_latency": max(latencies) if latencies else None,
            "latencies": latencies,
            "concurrent_tasks": concurrent_tasks,
            "num_workers": num_workers
        }
        
        # Store results in appropriate list based on test type
        results_list = f"{test_type}_test_results"
        if hasattr(self, results_list):
            getattr(self, results_list).append(metrics)
            
        return metrics
    
    def plot_performance_metrics(self, test_type="sleep"):
        """Plot performance metrics from stress tests"""
        results_list = f"{test_type}_test_results"
        if not hasattr(self, results_list):
            setattr(self, results_list, [])
        results = getattr(self, results_list)
        
        if not results:
            print(f"No {test_type} test results available for plotting")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f"{test_type.capitalize()} Test Performance Metrics", fontsize=16)

        # Create more informative labels that include total tasks
        labels = [f"{result['num_workers']} workers\n({result['concurrent_tasks']} procs)\n{result['total_tasks']} tasks" 
                for result in results]

        # Plot 1: Latency Distribution (Box Plot)
        latencies_data = [result["latencies"] for result in results]
        ax1.boxplot(latencies_data, labels=labels)
        ax1.set_title("Latency Distribution")
        ax1.set_ylabel("Latency (seconds)")
        ax1.set_xlabel("Configuration (workers & processes)")
        ax1.grid(True)

        # Plot 2: Success vs Failure Rate
        x = np.arange(len(results))
        width = 0.35
        success_rates = [result["successful_tasks"] for result in results]
        failure_rates = [result["failed_tasks"] for result in results]
        
        ax2.bar(x, success_rates, width, label='Successful')
        ax2.bar(x, failure_rates, width, bottom=success_rates, label='Failed')
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.set_title("Task Success vs Failure")
        ax2.set_ylabel("Number of Tasks")
        ax2.set_xlabel("Configuration (workers & processes)")
        ax2.legend()
        ax2.grid(True)

        # Plot 3: Average Latency Comparison
        avg_latencies = [result["avg_latency"] for result in results]
        ax3.plot(x, avg_latencies, 'o-', linewidth=2)
        ax3.set_xticks(x)
        ax3.set_xticklabels(labels)
        ax3.set_title("Average Latency by Configuration")
        ax3.set_ylabel("Average Latency (seconds)")
        ax3.set_xlabel("Configuration (workers & processes)")
        ax3.grid(True)

        # Plot 4: Latency Range
        min_latencies = [result["min_latency"] for result in results]
        max_latencies = [result["max_latency"] for result in results]
        median_latencies = [result["median_latency"] for result in results]

        ax4.fill_between(x, min_latencies, max_latencies, alpha=0.2, label='Range')
        ax4.plot(x, median_latencies, 'r-', label='Median', linewidth=2)
        ax4.set_xticks(x)
        ax4.set_xticklabels(labels)
        ax4.set_title("Latency Range by Configuration")
        ax4.set_ylabel("Latency (seconds)")
        ax4.set_xlabel("Configuration (workers & processes)")
        ax4.legend()
        ax4.grid(True)

        plt.tight_layout()
        return fig

    def plot_performance_metrics000(self, test_type="sleep"):
        """Plot performance metrics from stress tests"""
        results_list = f"{test_type}_test_results"
        if not hasattr(self, results_list):
            setattr(self, results_list, [])
        results = getattr(self, results_list)
        
        if not results:
            print(f"No {test_type} test results available for plotting")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f"{test_type.capitalize()} Test Performance Metrics", fontsize=16)

        # Create more informative labels
        labels = [f"{result['num_workers']} workers\n({result['concurrent_tasks']} procs)" for result in results]

        # Plot 1: Latency Distribution (Box Plot)
        latencies_data = [result["latencies"] for result in results]
        ax1.boxplot(latencies_data, labels=labels)
        ax1.set_title("Latency Distribution")
        ax1.set_ylabel("Latency (seconds)")
        ax1.set_xlabel("Configuration (workers & processes)")
        ax1.grid(True)

        # Plot 2: Success vs Failure Rate
        x = np.arange(len(results))
        width = 0.35
        success_rates = [result["successful_tasks"] for result in results]
        failure_rates = [result["failed_tasks"] for result in results]
        
        ax2.bar(x, success_rates, width, label='Successful')
        ax2.bar(x, failure_rates, width, bottom=success_rates, label='Failed')
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.set_title("Task Success vs Failure")
        ax2.set_ylabel("Number of Tasks")
        ax2.set_xlabel("Configuration (workers & processes)")
        ax2.legend()
        ax2.grid(True)

        # Plot 3: Average Latency Comparison
        avg_latencies = [result["avg_latency"] for result in results]
        ax3.plot(x, avg_latencies, 'o-', linewidth=2)
        ax3.set_xticks(x)
        ax3.set_xticklabels(labels)
        ax3.set_title("Average Latency by Configuration")
        ax3.set_ylabel("Average Latency (seconds)")
        ax3.set_xlabel("Configuration (workers & processes)")
        ax3.grid(True)

        # Plot 4: Latency Range
        min_latencies = [result["min_latency"] for result in results]
        max_latencies = [result["max_latency"] for result in results]
        median_latencies = [result["median_latency"] for result in results]

        ax4.fill_between(x, min_latencies, max_latencies, alpha=0.2, label='Range')
        ax4.plot(x, median_latencies, 'r-', label='Median', linewidth=2)
        ax4.set_xticks(x)
        ax4.set_xticklabels(labels)
        ax4.set_title("Latency Range by Configuration")
        ax4.set_ylabel("Latency (seconds)")
        ax4.set_xlabel("Configuration (workers & processes)")
        ax4.legend()
        ax4.grid(True)

        plt.tight_layout()
        return fig
