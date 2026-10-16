"""Run paired monthly history extraction with a persistent username set."""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import otacon.main as otacon_main
from otacon.argument_handling import handle_args
from otacon.prep_input import establish_timeframe


MONTH_PATTERN = re.compile(r"^(RC|RS)_(\d{4}-\d{2})(?:\..*)?$")


def define_runner_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract listed users' monthly comment and submission histories.")
    parser.add_argument("--comments-input", type=Path, required=True)
    parser.add_argument("--submissions-input", type=Path, required=True)
    parser.add_argument("--user-file", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--remote-root", required=True)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--pause-file", type=Path, required=True)
    parser.add_argument("--time-to", default="2022-12")
    return parser


def discover_month_files(input_dir: Path, prefix: str, time_to: str) -> dict:
    cutoff = tuple(int(part) for part in time_to.split("-"))
    files = {}

    for filename in establish_timeframe(None, cutoff, str(input_dir)):
        match = MONTH_PATTERN.match(filename)
        if match is None or match.group(1) != prefix:
            continue
        path = input_dir / filename
        if not path.is_file():
            continue
        month = match.group(2)
        if tuple(int(part) for part in month.split("-")) <= cutoff:
            files.setdefault(month, []).append(filename)

    return {month: sorted(month_files) for month, month_files in files.items()}


def _run_rclone(arguments: list, capture_output: bool = False) -> subprocess.CompletedProcess:
    shell_script = (
        'source ~/.zshrc >/dev/null 2>&1; '
        'typeset -f rclone-custom >/dev/null || exit 127; '
        'rclone-custom "$@"'
    )
    return subprocess.run(
        ["zsh", "-c", shell_script, "otacon-history-runner", *map(str, arguments)],
        check=False,
        text=True,
        capture_output=capture_output,
    )


def _move_pairs(pairs: list) -> None:
    shell_script = (
        'source ~/.zshrc >/dev/null 2>&1; '
        'typeset -f rclone-custom >/dev/null || exit 127; '
        'while (( $# >= 2 )); do '
        'rclone-custom move "$1" "$2" || exit $?; '
        'shift 2; '
        'done'
    )
    arguments = ["zsh", "-c", shell_script, "otacon-history-runner"]
    for source, destination in pairs:
        arguments.extend((str(source), str(destination)))
    subprocess.run(arguments, check=True)


def _load_otacon_args(config: argparse.Namespace):
    original_argv = sys.argv
    sys.argv = [
        "otacon-history-runner",
        "--input", str(config.comments_input),
        "--output", str(config.output_root),
        "--time_to", config.time_to,
        "--src", "user",
        "--name-file", str(config.user_file),
        "--return_all",
        "--no_stats",
        "--dont_filter",
        "--no_cleanup",
        "--reset-dedup-monthly",
    ]
    try:
        return handle_args()
    finally:
        sys.argv = original_argv


def _read_completed_months(state_file: Path) -> set:
    if not state_file.exists():
        return set()
    return {line.strip() for line in state_file.read_text(encoding="utf-8").splitlines() if line.strip()}


def _record_completed_month(state_file: Path, month: str) -> None:
    with state_file.open("a", encoding="utf-8") as state:
        state.write(month + "\n")
        state.flush()


def _run(config: argparse.Namespace) -> None:
    for directory in (config.comments_input, config.submissions_input):
        if not directory.is_dir():
            raise FileNotFoundError(f"Input directory is unavailable: {directory}")
    if not config.user_file.is_file():
        raise FileNotFoundError(f"Username file is unavailable: {config.user_file}")

    config.output_root.mkdir(parents=True, exist_ok=True)
    config.state_file.parent.mkdir(parents=True, exist_ok=True)

    comments = discover_month_files(config.comments_input, "RC", config.time_to)
    submissions = discover_month_files(config.submissions_input, "RS", config.time_to)
    months = sorted(set(comments) | set(submissions))
    if not months:
        raise RuntimeError(f"No monthly input files found through {config.time_to}.")

    completed = _read_completed_months(config.state_file)
    remote_listing = _run_rclone(["ls", config.remote_root], capture_output=True)
    if remote_listing.returncode == 0:
        marker_pattern = re.compile(
            rf"_otacon_history_(\d{{4}}-\d{{2}})_through_{re.escape(config.time_to)}_complete"
        )
        remote_completed = set(marker_pattern.findall(remote_listing.stdout))
        for month in sorted(remote_completed & set(months) - completed):
            print(f"Found completed {month} on NAS; recording local state.", flush=True)
            _record_completed_month(config.state_file, month)
        completed.update(remote_completed)

    incomplete_months = [month for month in months if month not in completed]
    if not incomplete_months:
        print(f"All available months through {config.time_to} are already complete.", flush=True)
        return

    args = _load_otacon_args(config)
    print(
        f"Loaded {len(args.name):,} usernames; processing {len(incomplete_months)} remaining months "
        f"through {config.time_to}.",
        flush=True,
    )

    for month in incomplete_months:
        if config.pause_file.exists():
            print(f"Pause requested before {month}; it is safe to eject the drive.", flush=True)
            return

        work_dir = Path(tempfile.mkdtemp(prefix=f".{month}.", dir=config.output_root))
        stream_outputs = []
        try:
            print(f"=== Processing {month} ===", flush=True)
            for stream, input_dir, month_files, prefix, searchmode, remote_dir in (
                ("submissions", config.submissions_input, submissions, "RS", "subs", f"{config.remote_root}/submissions"),
                ("comments", config.comments_input, comments, "RC", "comms", f"{config.remote_root}/comments"),
            ):
                input_files = month_files.get(month, [])
                if not input_files:
                    continue

                output_dir = work_dir / stream
                output_dir.mkdir()
                stream_outputs.append((output_dir, remote_dir))
                args.input = str(input_dir)
                args.output = str(output_dir)
                args.searchmode = searchmode

                for input_file in input_files:
                    outfile, reviewfile = otacon_main.open_files(args, input_file)
                    otacon_main.process_month(input_file, args, outfile, reviewfile)

            otacon_main.hash_set.clear()
            marker = work_dir / f"_otacon_history_{month}_through_{config.time_to}_complete"
            marker.touch()
            move_pairs = [(f"{output_dir}/", remote_dir) for output_dir, remote_dir in stream_outputs]
            move_pairs.append((marker, config.remote_root))
            _move_pairs(move_pairs)

            for output_dir, _ in stream_outputs:
                output_dir.rmdir()
            work_dir.rmdir()
            _record_completed_month(config.state_file, month)
            completed.add(month)
            print(f"=== Offloaded and recorded {month} ===", flush=True)
        finally:
            otacon_main.hash_set.clear()

        if config.pause_file.exists():
            print(f"Pause requested; {month} is complete and offloaded. It is safe to eject the drive.", flush=True)
            return

    print(f"All available months through {config.time_to} are offloaded to {config.remote_root}.", flush=True)


def main() -> None:
    config = define_runner_parser().parse_args()
    _run(config)


if __name__ == "__main__":
    main()