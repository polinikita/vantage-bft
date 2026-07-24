# Running Benchmarks
This document explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN).

## Local Benchmarks
When running benchmarks, the codebase is automatically compiled with the feature flag `benchmark`.

The local vehicle is the `node` binary's `local-benchmark` subcommand
(`node/src/local_benchmark.rs`): it self-hosts an entire run — every primary, every
worker, and one client task per worker — in a single OS process, reusing the exact
same `Primary::spawn`/`Worker::spawn`/`Client` code paths the standalone binaries
use. There is no Python/fabric/tmux orchestration on the local path; `fab` is used
only for the AWS/remote harness (see [AWS Benchmarks](#aws-benchmarks) below).

### Run the benchmark
Build once with the `benchmark` feature flag, then run:
```
$ cargo build --release --features benchmark
$ ./target/release/node local-benchmark --nodes 4 --workers 1 --rate 240000 \
      --tx-size 512 --protocol autobahn-optimistic --duration 60
```
Key flags (all have defaults — see `node local-benchmark --help`):
* `--nodes`: number of primaries (authorities) to spawn.
* `--workers`: workers per primary.
* `--rate`: aggregate input rate (tx/s), divided equally amongst the per-worker
  clients.
* `--tx-size`: transaction size in bytes.
* `--protocol`: `autobahn-optimistic`, `autobahn-seamless`, or `vantage`.
* `--duration`: benchmark duration in seconds.
* `--crash`: number of trailing nodes to leave unspawned (a true crash fault —
  committee membership is unchanged, only those nodes' tasks never start).
* `--delta-ms`, `--max-batch-delay-ms`, `--max-header-delay-ms`: protocol timing
  parameters (see `--help` for the exact semantics of each).
* `--mimic-latency-ms` / `--latency-table`: inject uniform or WAN-shaped
  (NxN RTT-ms CSV matrix) artificial network latency between authorities.

It generates keys, an in-memory committee, and `parameters.json`/`committee.json`
(written under `--data-dir`, default `.local-bench/`, for reference — nothing
re-reads them), runs the benchmark for `--duration` seconds, and prints a summary
computed in-process from each node's own Prometheus registry (no log parsing, no
scraping):
```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + RESULTS:
 Consensus TPS: 240071 tx/s
 Consensus BPS: 122916352 B/s

 Real transaction latency: avg 416.32 ms (stddev 58.10), p50/p90/p99 410.00/480.00/610.00 ms (14401920 txs, 0 misses)
-----------------------------------------
```
'Consensus TPS'/'Consensus BPS' report the committed throughput. 'Real transaction
latency' is the true end-to-end client-perceived latency, aggregated across every
node's own committed-transaction histogram (max for count/misses since every node
observes the same replicated commit stream, summed sum/sum-of-squares for the
avg/stddev ratio, median across nodes for percentiles).

An optional `monitoring/docker-compose.yml` Prometheus+Grafana stack can scrape the
running nodes live; `local-benchmark` prints the generated `prometheus.yaml` path
and the Grafana URL on startup.

## AWS Benchmarks
This repo integrates various python scripts to deploy and benchmark the codebase on [Amazon Web Services (AWS)](https://aws.amazon.com). They are particularly useful to run benchmarks in the WAN, across multiple data centers. This section provides a step-by-step tutorial explaining how to use them.

### Step 1. Set up your AWS credentials
Set up your AWS credentials to enable programmatic access to your account from your local machine. These credentials will authorize your machine to create, delete, and edit instances on your AWS account programmatically. First of all, [find your 'access key id' and 'secret access key'](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds). Then, create a file `~/.aws/credentials` with the following content:
```
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```
Do not specify any AWS region in that file as the python scripts will allow you to handle multiple regions programmatically.

### Step 2. Add your SSH public key to your AWS account
You must now [add your SSH public key to your AWS account](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). This operation is manual (AWS exposes little APIs to manipulate keys) and needs to be repeated for each AWS region that you plan to use. Upon importing your key, AWS requires you to choose a 'name' for your key; ensure you set the same name on all AWS regions. This SSH key will be used by the python scripts to execute commands and upload/download files to your AWS instances.
If you don't have an SSH key, you can create one using [ssh-keygen](https://www.ssh.com/ssh/keygen/):
```
$ ssh-keygen -f ~/.ssh/aws
```

### Step 3. Configure the testbed
The file [settings.json](https://github.com/facebookresearch/narwhal/blob/master/benchmark/settings.json) (located in [narwhal/benchmarks](https://github.com/facebookresearch/narwhal/blob/master/benchmark)) contains all the configuration parameters of the testbed to deploy. Its content looks as follows:
```json
{
    "key": {
        "name": "aws",
        "path": "/absolute/key/path"
    },
    "port": 5000,
    "repo": {
        "name": "narwhal",
        "url": "https://github.com/facebookresearch/narwhal.git",
        "branch": "master"
    },
    "instances": {
        "type": "m5d.8xlarge",
        "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
    }
}
```
The first block (`key`) contains information regarding your SSH key:
```json
"key": {
    "name": "aws",
    "path": "/absolute/key/path"
},
```
Enter the name of your SSH key; this is the name you specified in the AWS web console in step 2. Also, enter the absolute path of your SSH private key (using a relative path won't work). 


The second block (`ports`) specifies the TCP ports to use:
```json
"port": 5000,
```
Narwhal requires a number of TCP ports, depening on the number of workers per node, Each primary requires 2 ports (one to receive messages from other primaties and one to receive messages from its workers), and each worker requires 3 ports (one to receive client transactions, one to receive messages from its primary, and one to receive messages from other workers). Note that the script will open a large port range (5000-7000) to the WAN on all your AWS instances. 

The third block (`repo`) contains the information regarding the repository's name, the URL of the repo, and the branch containing the code to deploy: 
```json
"repo": {
    "name": "narwhal",
    "url": "https://github.com/facebookresearch/narwhal.git",
    "branch": "master"
},
```
Remember to update the `url` field to the name of your repo. Modifying the branch name is particularly useful when testing new functionalities without having to checkout the code locally. 

**`release_repo` (build-once/deploy-prebuilt-binary).** By default (no `--source-build` flag, see step 4/5 below), `fab install`/`fab remote`/`fab campaign` do **not** compile anything remotely: they download the `node`/`benchmark_client` binaries the repo's `.github/workflows/docker.yml` publishes to a rolling `nightly` GitHub Release on every push to `main`. Add a `release_repo` key to the `repo` block with your repo's `<OWNER>/<REPO>` slug:
```json
"repo": {
    "name": "vantage",
    "url": "https://github.com/<OWNER>/<REPO>.git",
    "branch": "main",
    "release_repo": "<OWNER>/<REPO>"
},
```
The repo must be public (anonymous `curl`, no auth). Without `release_repo` set, `fab remote`/`fab campaign` fail with a clear error telling you to either fill it in or pass `--source-build`.

The the last block (`instances`) specifies the [AWS instance type](https://aws.amazon.com/ec2/instance-types) and the [AWS regions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions) to use:
```json
"instances": {
    "type": "m5d.8xlarge",
    "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
}
```
The instance type selects the hardware on which to deploy the testbed. For example, `m5d.8xlarge` instances come with 32 vCPUs (16 physical cores), 128 GB of RAM, and guarantee 10 Gbps of bandwidth. The python scripts will configure each instance with 300 GB of SSD hard drive. The `regions` field specifies the data centers to use. If you require more nodes than data centers, the python scripts will distribute the nodes as equally as possible amongst the data centers. All machines run a fresh install of Ubuntu Server 20.04.

### Step 4. Create a testbed
The AWS instances are orchestrated with [Fabric](http://www.fabfile.org) from the file [fabfile.py](https://github.com/facebookresearch/narwhal/blob/master/benchmark/fabfile.pyy) (located in [narwhal/benchmarks](https://github.com/facebookresearch/narwhal/blob/master/benchmark)); you can list all possible commands as follows:
```
$ cd narwhal/benchmark
$ fab --list
```
The command `fab create` creates new AWS instances; open [fabfile.py](https://github.com/facebookresearch/narwhal/blob/master/benchmark/fabfile.py) and locate the `create` task:
```python
@task
def create(ctx, nodes=2):
    ...
```
The parameter `nodes` determines how many instances to create in *each* AWS region. That is, if you specified 5 AWS regions as in the example of step 3, setting `nodes=2` will creates a total of 10 machines:
```
$ fab create

Creating 10 instances |██████████████████████████████| 100.0% 
Waiting for all instances to boot...
Successfully created 10 new instances
```
You can then prepare the remote instances with `fab install`:
```
$ fab install

Initialized testbed of 10 nodes (fetch-binary mode)
```
By default this only installs runtime dependencies (curl, ca-certificates, tmux) -- no Rust toolchain, no source tree; `fab remote`/`fab campaign` fetch the pre-built `nightly` release binaries per run (see `release_repo` above). Pass `fab install --source-build` to fall back to the original behavior (full Rust toolchain + rsync'd working tree, remote `cargo build` on every run) -- useful when testing a change that hasn't been released yet; `fab remote --source-build`/`fab campaign --source-build` must then be used too, since the two toggles select incompatible host setups.
This may take a long time as the command will first update all instances.
The commands `fab stop` and `fab start` respectively stop and start the testbed without destroying it (it is good practice to stop the testbed when not in use as AWS can be quite expensive); and `fab destroy` terminates all instances and destroys the testbed. Note that, depending on the instance types, AWS instances may take up to several minutes to fully start or stop. The command `fab info` displays a nice summary of all available machines and information to manually connect to them (for debug).

### Step 5. Run a benchmark
After setting up the testbed, running a benchmark on AWS uses the same concepts as [Local Benchmarks](#local-benchmarks) above (nodes, workers, rate, tx size, faults, duration), just via the `fab` fabric harness instead of the `node local-benchmark` subcommand. Locate the task `remote` in [fabfile.py](https://github.com/facebookresearch/narwhal/blob/master/benchmark/fabfile.py):
```python
@task
def remote(ctx):
    ...
```
The benchmark parameters cover the same concepts as [local benchmarks](#local-benchmarks) but allow to specify the number of nodes and the input rate as arrays to automate multiple benchmarks with a single command. The parameter `runs` specifies the number of times to repeat each benchmark (to later compute the average and stdev of the results), and the parameter `collocate` specifies whether to collocate all the node's workers and the primary on the same machine. If `collocate` is set to `False`, the script will run one node per data center (AWS region), with its primary and each of its worker running on a dedicated instance.
```python
bench_params = {
    'nodes': [10, 20, 30],
    'workers: 2,
    'collocate': True,
    'rate': [20_000, 30_000, 40_000],
    'tx_size': 512,
    'faults': 0,
    'duration': 300,
    'runs': 2,
}
```
As with local benchmarks, the scripts will deploy as many clients as workers and divide the input rate equally amongst each client. Each client is colocated with a worker, and only submit transactions to the worker with whom they share the machine.

Once you specified both `bench_params` and `node_params` as desired, run:
```
$ fab remote
```
This command first updates all machines with the latest commit of the GitHub repo and branch specified in your file [settings.json](https://github.com/facebookresearch/narwhal/blob/master/benchmark/settings.json) (step 3); this ensures that benchmarks are always run with the latest version of the code. It then generates and uploads the configuration files to each machine, runs the benchmarks with the specified parameters, and downloads the logs. It finally parses the logs and prints the results into a folder called `results` (which is automatically created if it doesn't already exists). You can run `fab remote` multiple times without fearing to override previous results, the command either appends new results to a file containing existing ones or prints them in separate files. If anything goes wrong during a benchmark, you can always stop it by running `fab kill`.
 
### Step 6. Plot the results
Once you have enough results, you can aggregate and plot them:
```
$ fab plot
```
This command creates a latency graph, a throughput graph, and a robustness graph in a folder called `plots` (which is automatically created if it doesn't already exists). You can adjust the plot parameters to filter which curves to add to the plot:
```python
plot_params = {
    'faults': [0],
    'nodes': [10, 20, 50],
    'workers': [1],
    'collocate': True,
    'tx_size': 512,
    'max_latency': [3_500, 4_500]
}
```

The first graph ('latency') plots the latency versus the throughput. It shows that the latency is low until a fairly neat threshold after which it drastically increases. Determining this threshold is crucial to understand the limits of the system. 

Another challenge is comparing apples-to-apples between different deployments of the system. The challenge here is again that latency and throughput are interdependent, as a result a throughput/number of nodes chart could be tricky to produce fairly. The way to do it is to define a maximum latency and measure the throughput at this point instead of simply pushing every system to its peak throughput (where latency is meaningless). The second graph ('tps') plots the maximum achievable throughput under a maximum latency for different numbers of nodes.
