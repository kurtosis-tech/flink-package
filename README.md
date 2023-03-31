### Flink Package

This is a [Kurtosis Starlark Package](https://docs.kurtosis.com/explanations/starlark) that allows you to create an `n node` Flink Cluster. 
By default, it provisions 3 task managers.

### Run

This assumes you have the [Kurtosis CLI](https://docs.kurtosis.com/cli) installed

Simply run

```bash
kurtosis run github.com/kurtosis-tech/flink-package
```

If you want to override the number of task managers,

```
kurtosis run github.com/kurtosis-tech/flink-package '{"num_task_managers": <required_number_of_task_managers>}'
```

### Using this in your own package

Kurtosis Packages can be used within other Kurtosis Packages, through what we call composition internally. Assuming you want to spin up Flink and your own service
together you just need to do the following

```py
main_flink_module = import_module("github.com/kurtosis-tech/flink-package/main.star")

# main.star of your Flink + Service package
def run(plan, args):
    plan.print("Spinning up the Flink Package")
    # this will spin up Flink and return the output of the Flink package [flink-task-manager-0 .. flink-task-manager-n]
    flink_run_output = main_flink_module.run(plan, args)
```
