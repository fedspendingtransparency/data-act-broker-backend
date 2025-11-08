"""

Note: these are Broker duplicates of the scripts in brus_backend_common. The scripts in the package cannot run
out of the box because it needs the extra step to set its config values. The repo may evolve in the future to just run
scripts out of it directly without needing to go through the Broker or USAspending. Until then, we have to have these
wrapper scripts around their functionality.

"""
from brus_backend_common.config import set_brus_config
from brus_backend_common.scripts.load_query_to_delta import setup_parser, main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate a delta table with its designated query.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    # set_brus_config()
    main(args.table, args.incremental)