# Function As A Service

## MODELS
We have 3 models that execute all tasks put by the service in the redisDB. 
- Local dispatcher model
- Push model
- Pull model

# Setup
Install python dependencies to get started

```bash
pip install -r requirements.txt -q -U  # installs all requirements in the requirements.txt file
```
Make sure the redis server, fastapi server are setup before running the dispatchers and their corresponding workers or the testing scripts.
See the configuration guide at the bottom of the page for how to set up sessions for the servers and terminals

Each of the models perform task execution by dispatcher getting tasks from RedisDB.

Run a model dispatcher with this command. Select one of push, pull and local with -m and set the dispatcher port and number of workers. 


```bash
> python3 task_dispatcher.py -m [local/push/pull] -p <port> -w <num_worker_processors>
```


Also run a push/pull worker with these commands
```bash
> python3 pull_worker.py <num_worker_processors> <dispatcher url>
> python3 push_worker.py <num_worker_processors> <dispatcher url>
```

# Instructions for running webservice and performance tests
There are 5 testing scripts.  
The following show how to run each of them for webservice and performance testing

## WEBSERVICE TESTING

### Run webservice tests for push

1. Clear the jobs currently running as well as all data stored in the redis db
Kill all workers and dispatchers and flush redisdb
```bash
redis-cli flushall # in the redis cli session
```

2. start a dispatcher in the tmux dispatcher session
```bash
python3 task_dispatcher.py -m push # in a dispatcher tmux session
```

3. Start a new worker(s) in the tmux worker session
  Before running the pytests make sure there to have a worker running
```bash
python3 push_model/push_worker.py <num_processes> <dispatcher_url> 
python3 push_model/push_worker.py 4 tcp://127.0.0.1:5556
```

4. run this in the redis tmux session
```bash
pytest -s test_webservice_push.py
```


### Run webservice tests for pull

1. Clear the jobs currently running as well as all data stored in the redis db
Kill all workers and dispatchers and flush redisdb
```bash
redis-cli flushall # in the redis cli session
```

2. start a dispatcher in the tmux dispatcher session
```bash
python3 task_dispatcher.py -m pull # in a dispatcher tmux session
```

3. Start a new worker(s) in the tmux worker session
```bash
python3 pull_model/pull_worker.py <num_processes> <dispatcher_url> 
python3 pull_model/pull_worker.py 4 tcp://127.0.0.1:5556
```

4. run this in the redis tmux session
```bash
pytest -s test_webservice_pull.py
```


### Run webservice tests for local
1. Run local dispatcher before webservice test
```bash
python3 task_dispatcher.py -m local
```

2. 
```bash
pytest -s test_webservice_local.py
```


# PERFORMANCE TESTING

Select a configuration for for workers and also tasks. The main block contains commented configurations 
that can be uncommented to run for weak scaling and load testing or edited for other configgurations -- see other configuration functions in the script. 

All plots will be saved to "./testing_plots/comparative"

### Run performance tests for local

1. Run a local dispatcher before running the performance test
```bash
python3 task_dispatcher.py -m local &
```
2. 
```bash
python3 test_local_dispatcher_performance.py
```


### Run performance tests for pull
1.  Run a pull dispatcher before running the performance test
```bash
python3 task_dispatcher.py -m pull &
```

2. 
```bash
python3 test_push_pull_performance.py -m pull
```

### Run performance tests for push
1. Run a push dispatcher before running the performance test
```bash
python3 task_dispatcher.py -m push &
```

2.
```bash
python3 test_push_pull_performance.py -m push
```
### LEARNING For future improvements:
We could have done a class model for tests instead of recreating the tests multiple times. 
We could have made instances of local, pull, and push models.

Similarly, for performance testing, we could have build a class for performance testing 
and built instances of local, push, and pull models to demonstrate performance.

The -w command for starting the task_dispatcher could be improved/implemented to start the workers for all the dispatchers in the task_dispatcher.py script. 
This would simply spin the corresponding worker for the specified dispatcher. Not implemented in due to time.


### HELPER SCRIPTS
- my_faas_client.py : sets up client to talk to service. The testing scripts for perfomance import this file to run tests on service

- custom_exceptions.py : contains the exceptions we have for communicating failures to client
- serialize.py : has the serialize/deserialize functions
- test_functions.py : contains custom functions for testing the service with pytests


# CONFIGURATION GUIDE

## REDIS
$ brew install redis
this might take 5 minutes

start in background
$ brew services start redis

when running, should show:
redis (homebrew.mxcl.redis)
Running: ✔
Loaded: ✔
User: {usename}
PID: {67975}

stop redis
$ brew services stop redis

test if running
$ redis-cli
should return the local host for you to type

test using PING command
$ 127.0.0.1:6379> ping
Should receive back $ PONG

test register func script
-- to test use testing_and_reports/test_register.py
-- PORT: 7777

## ZMQ
```bash
$ pip install pyzmq
this might take 1-2 mins
```


## Commands for Managing Sessions

Command order to set up and manage tmux sessions.

---

### Redis Session
- Start a new `tmux` session for Redis:
  ```bash
  tmux new-session -s redis
  ```
- Run Redis in the background:
  ```bash
  redis-server &
  ```

- Run tests in the Redis session:
  ```bash
  python3 test_webservice_[push/pull/local].py
  ```

- Kill the Redis session:
  ```bash
  tmux kill-session -t redis
  ```
- Clear all data in Redis:
  ```bash
  redis-cli flushall
  ```

---

### FastAPI Session
- Start a new `tmux` session for FastAPI:
  ```bash
  tmux new-session -s fastapi
  ```
- Start the FastAPI server:
  ```bash
  uvicorn REST:app --reload
  ```
- Kill the FastAPI session:
  ```bash
  tmux kill-session -t fastapi
  ```

---

### Dispatcher Session
- Start a new `tmux` session for the dispatcher:
  ```bash
  tmux new-session -s dispatcher
  ```
- Run the dispatcher:
  ```bash
  python3 task_dispatcher.py -m [local/push/pull] -p <port number> -w <num workers>
  ```
- Kill the dispatcher session:
  ```bash
  tmux kill-session -t dispatcher
  ```

---

### Client Testing (Run Last)
Run this command in the Redis session after all other sessions are set up:
```bash
python3 test_webservice_[push/pull/local].py
```

---

## TMUX Common Commands
- See all active tmux sessions:
  ```bash
  tmux ls
  ```
- Start a new tmux session:
  ```bash
  tmux new-session -s <session_name>
  ```
- Detach from a session:
  ```
  Ctrl+B, then D
  ```
- Reattach to a session after detaching:
  ```bash
  tmux attach-session -t <session_name>
  ```
- Kill a session:
  ```bash
  tmux kill-session -t <session_name>
  ```

---

### Scrolling Terminal in a tmux Session
- Enter scroll mode:
  ```
  Ctrl+B, then [
  ```
- Scroll with arrow keys
- Press `q` to quit scrolling

---

# Debugging Commands
- List running tmux processes:
  ```bash
  tmux ls
  ```
- Kill a running script by filename:
  ```bash
  pkill -f <file_name>
  ```
- See all processes running with a name/PID/etc.:
  ```bash
  ps aux | grep <process_name/PID/etc.>
  ```
- Find which processes/PIDs are on a port:
  ```bash
  lsof -i :<port_num>
  ```
- Kill a process by PID:
  ```bash
  kill -9 <PID>
  ```

