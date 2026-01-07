#!/usr/bin/env python3
"""
Plan Runner for Weaviate Benchmarking

Automates the process of running benchmarks against control and candidate
branches of Weaviate based on a YAML plan file.
"""

import argparse
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import requests
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class BenchmarkError(Exception):
    """Custom exception for benchmark-related errors."""
    pass


class WeaviateError(Exception):
    """Custom exception for Weaviate-related errors."""
    pass


@dataclass
class RunConfig:
    """Configuration for a single benchmark run."""
    name: str
    ingest_on: str  # 'control' or 'candidate'
    parameters: List[str] = field(default_factory=list)
    async_indexing: bool = False


@dataclass
class PlanConfig:
    """Configuration loaded from the YAML plan file."""
    path_to_weaviate_repo: str
    control_branch: str
    candidate_branch: str
    global_parameters: List[str]
    runs: List[RunConfig]

    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'PlanConfig':
        """Load configuration from a YAML file."""
        path = Path(yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"Plan file not found: {yaml_path}")

        with open(path, 'r') as f:
            data = yaml.safe_load(f)

        # Parse runs
        runs = []
        for run_data in data.get('runs', []):
            runs.append(RunConfig(
                name=run_data['name'],
                ingest_on=run_data.get('ingest_on', 'control'),
                parameters=run_data.get('parameters', []),
                async_indexing=run_data.get('async_indexing', False)
            ))

        return cls(
            path_to_weaviate_repo=os.path.expanduser(data['path_to_weaviate_repo']),
            control_branch=data['control_branch'],
            candidate_branch=data['candidate_branch'],
            global_parameters=data.get('global_parameters', []),
            runs=runs
        )


class WeaviateManager:
    """Manages Weaviate process lifecycle and branch switching."""

    def __init__(self, repo_path: str, data_path: str = "./data"):
        self.repo_path = Path(repo_path).resolve()
        self.data_path = data_path
        self.process: Optional[subprocess.Popen] = None
        self.current_branch: Optional[str] = None
        self._log_file_handle = None

    def _kill_existing_weaviate(self) -> None:
        """Kill any existing Weaviate processes on port 8080."""
        my_pid = os.getpid()
        try:
            # Find processes LISTENING on port 8080 (not clients)
            # Using -sTCP:LISTEN to filter only listeners
            result = subprocess.run(
                ["lsof", "-ti", "TCP:8080", "-sTCP:LISTEN"],
                capture_output=True,
                text=True
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid_str in pids:
                    if pid_str:
                        try:
                            pid = int(pid_str)
                            # Don't kill ourselves (shouldn't happen, but be safe)
                            if pid == my_pid:
                                logger.debug(f"Skipping own process (PID: {pid})")
                                continue
                            logger.warning(f"Killing existing process on port 8080 (PID: {pid})")
                            os.kill(pid, signal.SIGKILL)
                        except (ProcessLookupError, ValueError):
                            pass
                # Wait a moment for the port to be released
                time.sleep(2)
        except FileNotFoundError:
            # lsof not available, try alternative
            pass

    def checkout_branch(self, branch: str) -> None:
        """Switch to the specified git branch."""
        if self.current_branch == branch:
            logger.info(f"Already on branch {branch}")
            return

        logger.info(f"Switching to branch: {branch}")

        # Fetch latest to ensure branch exists
        subprocess.run(
            ["git", "fetch", "--all"],
            cwd=self.repo_path,
            check=True,
            capture_output=True
        )

        # Checkout the branch
        subprocess.run(
            ["git", "checkout", branch],
            cwd=self.repo_path,
            check=True,
            capture_output=True,
            text=True
        )

        self.current_branch = branch
        logger.info(f"Switched to branch: {branch}")

    def _build_weaviate(self) -> Path:
        """Build Weaviate binary if needed. Returns path to binary."""
        binary_path = Path(self.repo_path) / "weaviate-server"

        # Always rebuild after branch switch to ensure correct version
        logger.info(f"Building Weaviate binary for branch {self.current_branch}...")
        result = subprocess.run(
            ["go", "build", "-o", "weaviate-server", "./cmd/weaviate-server"],
            cwd=self.repo_path,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f"Build failed: {result.stderr}")
            raise WeaviateError(f"Failed to build Weaviate: {result.stderr}")
        logger.info("Weaviate binary built successfully")

        return binary_path

    def start(self, async_indexing: bool = False) -> None:
        """Start the Weaviate server from source.

        Args:
            async_indexing: Enable async indexing if True
        """
        if self.process is not None:
            raise WeaviateError("Weaviate is already running")

        # Kill any stale Weaviate processes first
        self._kill_existing_weaviate()

        # Build the binary first (much faster than go run)
        binary_path = self._build_weaviate()

        logger.info(f"Starting Weaviate from {self.repo_path} (async_indexing={async_indexing})")

        # Environment variables as specified in requirements
        env = os.environ.copy()
        env.update({
            "HNSW_STARTUP_WAIT_FOR_VECTOR_CACHE": "true",
            "DISABLE_LAZY_LOAD_SHARDS": "true",
            "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
            "CLUSTER_IN_LOCALHOST": "true",
            "DEFAULT_VECTORIZER_MODULE": "none",
            "PERSISTENCE_DATA_PATH": str(Path(self.repo_path) / self.data_path),
            "RAFT_BOOTSTRAP_EXPECT": "1",
            "ASYNC_INDEXING": "true" if async_indexing else "false",
        })

        cmd = [
            str(binary_path),
            "--scheme", "http",
            "--host", "127.0.0.1",
            "--port", "8080"
        ]

        # Write Weaviate logs to a file instead of piping (which can block)
        log_file = Path(self.repo_path) / "weaviate.log"
        self._log_file_handle = open(log_file, "w")

        self.process = subprocess.Popen(
            cmd,
            cwd=self.repo_path,
            env=env,
            stdout=self._log_file_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # Give Weaviate its own process session
        )

        logger.info(f"Weaviate started with PID: {self.process.pid} (logs: {log_file})")

    def stop(self) -> None:
        """Stop the Weaviate server gracefully."""
        if self.process is None:
            logger.warning("No Weaviate process to stop")
            return

        logger.info("Stopping Weaviate...")

        # Since we use start_new_session=True, we need to kill the process group
        try:
            pgid = os.getpgid(self.process.pid)
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            # Process already gone
            pass

        try:
            self.process.wait(timeout=30)
            logger.info("Weaviate stopped gracefully")
        except subprocess.TimeoutExpired:
            logger.warning("Weaviate did not stop gracefully, sending SIGKILL")
            try:
                pgid = os.getpgid(self.process.pid)
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            self.process.wait()

        self.process = None

        # Close the log file handle
        if self._log_file_handle is not None:
            self._log_file_handle.close()
            self._log_file_handle = None

    def _is_process_alive(self) -> bool:
        """Check if the Weaviate process is still running."""
        if self.process is None:
            return False
        return self.process.poll() is None

    def wait_for_ready(self, timeout: int = 300, poll_interval: float = 2.0,
                       required_consecutive: int = 3) -> bool:
        """Poll readiness endpoint until Weaviate is ready.

        Args:
            timeout: Maximum time to wait in seconds
            poll_interval: Time between polls in seconds
            required_consecutive: Number of consecutive successful checks required
        """
        url = "http://localhost:8080/v1/.well-known/ready"
        start_time = time.time()
        consecutive_success = 0

        logger.info("Waiting for Weaviate to be ready...")

        while time.time() - start_time < timeout:
            # First check if our process is still alive
            if not self._is_process_alive():
                logger.error("Weaviate process has died")
                # Try to get exit code and any output
                if self.process is not None:
                    exit_code = self.process.poll()
                    logger.error(f"Process exit code: {exit_code}")
                return False

            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    consecutive_success += 1
                    if consecutive_success >= required_consecutive:
                        logger.info(f"Weaviate is ready (verified {required_consecutive} times)")
                        return True
                    logger.debug(f"Ready check passed ({consecutive_success}/{required_consecutive})")
                else:
                    consecutive_success = 0
            except requests.exceptions.RequestException as e:
                consecutive_success = 0
                logger.debug(f"Ready check failed: {e}")

            time.sleep(poll_interval)

        logger.error(f"Weaviate did not become ready within {timeout} seconds")
        return False

    def clear_data(self) -> None:
        """Clear Weaviate data directory for fresh start."""
        data_path = Path(self.repo_path) / self.data_path
        if data_path.exists():
            logger.info(f"Clearing data directory: {data_path}")
            shutil.rmtree(data_path)
        data_path.mkdir(parents=True, exist_ok=True)


class BenchmarkRunner:
    """Runs the benchmarker tool and manages results."""

    def __init__(self, benchmarker_path: str, results_path: str = "./results"):
        self.benchmarker_path = Path(benchmarker_path).resolve()
        self.results_path = Path(results_path).resolve()

    def run_ingest(self, global_params: List[str], run_params: List[str]) -> None:
        """Run benchmarker for ingest (without -q flag)."""
        cmd = ["go", "run", ".", "ann-benchmark"]

        # Add global parameters
        cmd.extend(self._expand_params(global_params))

        # Add run-specific parameters
        cmd.extend(self._expand_params(run_params))

        logger.info(f"Running ingest: {' '.join(cmd)}")

        try:
            subprocess.run(
                cmd,
                cwd=self.benchmarker_path,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise BenchmarkError(f"Ingest failed with exit code {e.returncode}") from e

    def run_query(self, global_params: List[str], run_params: List[str],
                  run_id: str) -> None:
        """Run benchmarker in query-only mode with labels."""
        cmd = ["go", "run", ".", "ann-benchmark"]

        # Add query-only flag
        cmd.append("-q")

        # Add run_id label
        cmd.append(f"--labels=run_id={run_id}")

        # Add global parameters
        cmd.extend(self._expand_params(global_params))

        # Add run-specific parameters
        cmd.extend(self._expand_params(run_params))

        logger.info(f"Running query ({run_id}): {' '.join(cmd)}")

        try:
            subprocess.run(
                cmd,
                cwd=self.benchmarker_path,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise BenchmarkError(f"Query run failed with exit code {e.returncode}") from e

    def _expand_params(self, params: List[str]) -> List[str]:
        """Expand parameters, handling tilde expansion in paths."""
        expanded = []
        for param in params:
            # Handle tilde expansion for file paths
            if "~" in param and "=" in param:
                key, value = param.split("=", 1)
                value = os.path.expanduser(value)
                expanded.append(f"{key}={value}")
            else:
                expanded.append(os.path.expanduser(param))
        return expanded

    def clear_results(self) -> None:
        """Clear the results directory."""
        if self.results_path.exists():
            logger.info(f"Clearing results directory: {self.results_path}")
            for file in self.results_path.glob("*.json"):
                file.unlink()

    def copy_results(self, destination: str) -> None:
        """Copy results to a destination folder for archival."""
        dest_path = Path(destination)
        dest_path.mkdir(parents=True, exist_ok=True)

        for file in self.results_path.glob("*.json"):
            shutil.copy2(file, dest_path / file.name)

        logger.info(f"Copied results to: {dest_path}")


class VisualizationGenerator:
    """Generates visualizations from benchmark results."""

    def __init__(self, visualize_script: str = "./visualize.py"):
        self.visualize_script = Path(visualize_script).resolve()

    def generate(self, output_name: str, title: str = None, subtitle: str = None,
                 control_branch: str = None, candidate_branch: str = None) -> None:
        """Generate visualization and save with the specified name.

        Args:
            output_name: Name for the output PNG file
            title: Custom title for the plot (run name)
            subtitle: Custom subtitle for the plot (parameters)
            control_branch: Control branch name
            candidate_branch: Candidate branch name
        """
        logger.info(f"Generating visualization: {output_name}")

        # Build command with optional title/subtitle/branches
        cmd = ["python3", str(self.visualize_script)]
        if title:
            cmd.extend(["--title", title])
        if subtitle:
            cmd.extend(["--subtitle", subtitle])
        if control_branch:
            cmd.extend(["--control-branch", control_branch])
        if candidate_branch:
            cmd.extend(["--candidate-branch", candidate_branch])

        # Run the existing visualize.py script
        subprocess.run(
            cmd,
            cwd=self.visualize_script.parent,
            check=True,
            capture_output=True,
            text=True
        )

        # Rename output.png to the specified name in visualizations subdirectory
        output_png = self.visualize_script.parent / "output.png"
        if output_png.exists():
            viz_dir = self.visualize_script.parent / "visualizations"
            viz_dir.mkdir(exist_ok=True)
            target = viz_dir / f"{output_name}.png"
            output_png.rename(target)
            logger.info(f"Visualization saved to: {target}")


class PlanExecutor:
    """Orchestrates the execution of the benchmark plan."""

    def __init__(self, plan: PlanConfig, benchmarker_path: str,
                 output_dir: str = "./results_archive",
                 ready_timeout: int = 300,
                 skip_visualization: bool = False,
                 dry_run: bool = False):
        self.plan = plan
        self.weaviate = WeaviateManager(plan.path_to_weaviate_repo)
        self.benchmarker = BenchmarkRunner(benchmarker_path)
        self.visualizer = VisualizationGenerator()
        self.output_dir = output_dir
        self.ready_timeout = ready_timeout
        self.skip_visualization = skip_visualization
        self.dry_run = dry_run
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown on SIGINT/SIGTERM."""
        def signal_handler(signum, frame):
            logger.warning(f"Received signal {signum}, initiating graceful shutdown...")
            self.weaviate.stop()
            sys.exit(1)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def execute(self) -> None:
        """Execute all runs in the plan."""
        logger.info(f"Starting plan execution with {len(self.plan.runs)} runs")

        if self.dry_run:
            self._print_dry_run()
            return

        try:
            for i, run in enumerate(self.plan.runs, 1):
                logger.info(f"=== Run {i}/{len(self.plan.runs)}: {run.name} ===")
                self.execute_run(run)
                logger.info(f"=== Completed run: {run.name} ===")
        except Exception as e:
            logger.error(f"Plan execution failed: {e}")
            raise
        finally:
            # Ensure Weaviate is stopped on exit
            self.weaviate.stop()

        logger.info("Plan execution completed successfully")

    def _print_dry_run(self) -> None:
        """Print what would be executed without actually running."""
        for i, run in enumerate(self.plan.runs, 1):
            ingest_branch = (
                self.plan.control_branch if run.ingest_on == "control"
                else self.plan.candidate_branch
            )

            print(f"\n=== Run {i}: {run.name} (async_indexing={run.async_indexing}) ===")
            print(f"\n[INGEST PHASE on {ingest_branch}]")
            print(f"  git checkout {ingest_branch}")
            print(f"  Clear data directory")
            print(f"  Start Weaviate (ASYNC_INDEXING={run.async_indexing})")
            ingest_cmd = ["go", "run", ".", "ann-benchmark"]
            ingest_cmd.extend(self.benchmarker._expand_params(self.plan.global_parameters))
            ingest_cmd.extend(self.benchmarker._expand_params(run.parameters))
            print(f"  {' '.join(ingest_cmd)}")
            print(f"  Clear results/")

            print(f"\n[CONTROL QUERY PHASE on {self.plan.control_branch}]")
            print(f"  git checkout {self.plan.control_branch}")
            print(f"  Start Weaviate (ASYNC_INDEXING={run.async_indexing})")
            query_cmd = ["go", "run", ".", "ann-benchmark", "-q", "--labels=run_id=control"]
            query_cmd.extend(self.benchmarker._expand_params(self.plan.global_parameters))
            query_cmd.extend(self.benchmarker._expand_params(run.parameters))
            print(f"  {' '.join(query_cmd)}")

            print(f"\n[CANDIDATE QUERY PHASE on {self.plan.candidate_branch}]")
            print(f"  git checkout {self.plan.candidate_branch}")
            print(f"  Start Weaviate (ASYNC_INDEXING={run.async_indexing})")
            query_cmd = ["go", "run", ".", "ann-benchmark", "-q", "--labels=run_id=candidate"]
            query_cmd.extend(self.benchmarker._expand_params(self.plan.global_parameters))
            query_cmd.extend(self.benchmarker._expand_params(run.parameters))
            print(f"  {' '.join(query_cmd)}")

            safe_name = run.name.replace(" ", "_").lower()
            print(f"\n[VISUALIZATION]")
            print(f"  python3 visualize.py -> visualizations/benchmark_{safe_name}.png")
            print(f"  Archive results to {self.output_dir}/{safe_name}/")

    def execute_run(self, run: RunConfig) -> None:
        """Execute a single run configuration."""
        # Determine which branch to use for ingest
        ingest_branch = (
            self.plan.control_branch if run.ingest_on == "control"
            else self.plan.candidate_branch
        )

        # Step 1: Ingest phase
        logger.info(f"Starting ingest on branch: {ingest_branch}")
        self._start_weaviate_on_branch(
            ingest_branch,
            clear_data=True,
            async_indexing=run.async_indexing
        )

        self.benchmarker.run_ingest(
            self.plan.global_parameters,
            run.parameters
        )

        # Step 2: Clear results after ingest (we don't want ingest results)
        self.benchmarker.clear_results()

        # Step 3: Query with control branch
        logger.info("Starting query phase on control branch")
        self._stop_and_start_weaviate(
            self.plan.control_branch,
            async_indexing=run.async_indexing
        )

        self.benchmarker.run_query(
            self.plan.global_parameters,
            run.parameters,
            run_id="control"
        )

        # Step 4: Query with candidate branch
        logger.info("Starting query phase on candidate branch")
        self._stop_and_start_weaviate(
            self.plan.candidate_branch,
            async_indexing=run.async_indexing
        )

        self.benchmarker.run_query(
            self.plan.global_parameters,
            run.parameters,
            run_id="candidate"
        )

        # Step 5: Generate visualization for this run
        if not self.skip_visualization:
            safe_name = run.name.replace(" ", "_").lower()

            # Build subtitle from all parameters used
            all_params = self.plan.global_parameters + run.parameters
            # Format parameters nicely, removing flag prefixes for readability
            param_strs = []
            for param in all_params:
                # Clean up parameter formatting
                if param.startswith('--'):
                    param_strs.append(param[2:])  # Remove --
                elif param.startswith('-'):
                    param_strs.append(param[1:])   # Remove -
                else:
                    param_strs.append(param)
            subtitle = ", ".join(param_strs)

            self.visualizer.generate(
                f"benchmark_{safe_name}",
                title=run.name,
                subtitle=subtitle,
                control_branch=self.plan.control_branch,
                candidate_branch=self.plan.candidate_branch
            )

        # Step 6: Archive results for this run
        safe_name = run.name.replace(" ", "_").lower()
        self.benchmarker.copy_results(f"{self.output_dir}/{safe_name}")

        # Step 7: Stop Weaviate and clear results for next run
        self.weaviate.stop()
        self.benchmarker.clear_results()

    def _start_weaviate_on_branch(self, branch: str, clear_data: bool = False,
                                   async_indexing: bool = False) -> None:
        """Helper to start Weaviate on a specific branch."""
        self.weaviate.checkout_branch(branch)

        if clear_data:
            self.weaviate.clear_data()

        self.weaviate.start(async_indexing=async_indexing)

        if not self.weaviate.wait_for_ready(timeout=self.ready_timeout):
            raise WeaviateError(f"Weaviate failed to start on branch {branch}")

    def _stop_and_start_weaviate(self, branch: str, async_indexing: bool = False) -> None:
        """Stop Weaviate, switch branch, and restart (keeping data)."""
        self.weaviate.stop()
        self.weaviate.checkout_branch(branch)
        self.weaviate.start(async_indexing=async_indexing)

        if not self.weaviate.wait_for_ready(timeout=self.ready_timeout):
            raise WeaviateError(f"Weaviate failed to start on branch {branch}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Run Weaviate benchmarks based on a YAML plan',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python plan_runner.py plan.yml
  python plan_runner.py plan.yml --dry-run
  python plan_runner.py plan.yml -v --output-dir ./my_results
        """
    )

    parser.add_argument(
        'plan_file',
        help='Path to the YAML plan file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print commands without executing'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--output-dir',
        default='./results_archive',
        help='Directory for archived results (default: ./results_archive)'
    )
    parser.add_argument(
        '--skip-visualization',
        action='store_true',
        help='Skip visualization generation'
    )
    parser.add_argument(
        '--ready-timeout',
        type=int,
        default=300,
        help='Timeout in seconds for Weaviate readiness (default: 300)'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        plan = PlanConfig.from_yaml(args.plan_file)
        executor = PlanExecutor(
            plan,
            benchmarker_path=".",
            output_dir=args.output_dir,
            ready_timeout=args.ready_timeout,
            skip_visualization=args.skip_visualization,
            dry_run=args.dry_run
        )

        executor.execute()

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except (BenchmarkError, WeaviateError) as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)


if __name__ == '__main__':
    main()
