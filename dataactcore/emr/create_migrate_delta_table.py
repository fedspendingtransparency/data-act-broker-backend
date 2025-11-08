"""

Note: these are Broker duplicates of the scripts in brus_backend_common. The scripts in the package cannot run
out of the box because it needs the extra step to set its config values. The repo may evolve in the future to just run
scripts out of it directly without needing to go through the Broker or USAspending. Until then, we have to have these
wrapper scripts around their functionality.

"""
from brus_backend_common.config import set_brus_config
from brus_backend_common.scripts.create_migrate_delta_table import setup_parser, main

from dataactcore.config import CONFIG_BROKER

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or migrate delta tables")
    parser = setup_parser(parser)
    args = parser.parse_args()

    set_brus_config({
        'IS_LOCAL': not CONFIG_BROKER["use_aws"],
    })
    main(args.table, args.recreate, args.migrate)
