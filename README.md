# Analysis

A simple Python daemon that schedules tasks to run on a regular interval. We use this daemon in our data visualization projects to perform intermediate analysis on large datasets.

## Installation

This daemon depends on the [schedule](https://schedule.readthedocs.io/en/stable/) package to run tasks on a regular interval. You can install it on your local machine with the command below:

```bash
pip install schedule
```

## Usage

### Running

To run the daemon, just execute:
```
python main.py
```

It is up to you how you go about configuring your system to run `main.py` on startup. See [this StackOverflow post](https://stackoverflow.com/questions/24518522/run-python-script-at-startup-in-ubuntu) if you are using Ubuntu for guidance.

### Adding a task

To add a task, follow the three steps below:

1. Tasks are Python files with a single `run` function that gets executed when the task is executed. See [tasks/example.py](tasks/example.py):
    ```python
    def run():
        print("Hello, World!")
    ```

2. Import your task to [main.py](main.py):

    ```python
    from tasks import example
    ```

3. Schedule the task using the [schedule](https://schedule.readthedocs.io/en/stable/index.html) package. You can find the docs [here](https://schedule.readthedocs.io/en/stable/index.html).

    ```python
    # Schedules the `example` task to execute every 3 seconds.
    schedule.every(3).seconds.do(run_threaded, example)
    ```

## License

This project is licensed under the [MIT license](/LICENSE) by the Technologies & International Development (TID) Lab at Georgia Tech.