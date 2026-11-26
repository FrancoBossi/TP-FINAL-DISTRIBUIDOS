"""Inject, monitor, and benchmark Compose faults."""

import argparse

from scripts import chaos_monkey, monitor_chaos, run_chaos_matrix, sweep_chaos_intervals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, module in (
        ("inject", chaos_monkey),
        ("monitor", monitor_chaos),
        ("sweep", sweep_chaos_intervals),
        ("matrix", run_chaos_matrix),
    ):
        command = commands.add_parser(name, help=module.__doc__)
        module.add_arguments(command)
        command.set_defaults(handler=module.run, command_parser=command)

    args = parser.parse_args()
    return args.handler(args, args.command_parser)


if __name__ == "__main__":
    raise SystemExit(main())
