import logging
import argparse

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def create_matviews(sess):
    logger.info("Creating zip_county temporary view")

    # zip_county view creation
    sess.execute(
        """CREATE MATERIALIZED VIEW zip_county AS (
            SELECT
                concat(zip5, zip_last4) AS combined_zip,
                concat(zip5, '-', zip_last4) AS dashed_zip,
                zip5,
                zip_last4,
                zips.county_number,
                county_name
            FROM zips
            LEFT OUTER JOIN county_code AS cc
                ON cc.state_code = zips.state_abbreviation
                AND cc.county_number = zips.county_number)"""
    )
    sess.commit()

    logger.info("Created zip_county temporary view, creating zip_county index on zip5")
    sess.execute("CREATE INDEX ix_zip5_zip_county ON zip_county (zip5)")

    logger.info("Created zip_county index on zip5, creating zip_county index on zip_last4")
    sess.execute("CREATE INDEX ix_zip_last4_zip_county ON zip_county (zip_last4)")

    logger.info("Created zip_county index on zip_last4, creating zip_county index on combined_zip")
    sess.execute("CREATE INDEX ix_combined_zip_zip_county ON zip_county (combined_zip)")

    logger.info("Created zip_county index on combined_zip, creating zip_county index on dashed_zip")
    sess.execute("CREATE INDEX ix_dashed_zip_zip_county ON zip_county (dashed_zip)")
    sess.commit()

    logger.info("Created zip_county indexes, creating single_county temporary view")

    # single_county view creation
    sess.execute(
        """CREATE MATERIALIZED VIEW single_county AS (
            SELECT zip5, county_number, county_name
            FROM (SELECT
                zip5,
                county_number,
                county_name,
                ROW_NUMBER() OVER (PARTITION BY
                    zip5) AS row
                FROM zip_county) AS tmp
                WHERE tmp.row = 1)"""
    )
    sess.commit()

    logger.info("Created single_county temporary view, creating single_county index on zip5")

    sess.execute("CREATE INDEX ix_zip5_single_county ON single_county (zip5)")
    sess.commit()

    logger.info("Created single_county index, matview creation complete.")


def delete_matviews(sess):
    logger.info("Deleting matviews")
    # zip_county view deletion
    sess.execute("DROP MATERIALIZED VIEW IF EXISTS single_county")
    sess.execute("DROP MATERIALIZED VIEW IF EXISTS zip_county")
    sess.commit()

    logger.info("Finished delete of matviews.")


def update_fpds_le(sess):
    logger.info("Starting FPDS legal entity derivations, starting legal entity 9-digit zips without dashes")
    # FPDS LE 9-digit no dash
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET legal_entity_county_code = zc.county_number,
                legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                THEN dap.legal_entity_county_name
                                                ELSE UPPER(zc.county_name) END
            FROM zip_county AS zc
            WHERE zc.combined_zip = dap.legal_entity_zip4
                AND dap.legal_entity_county_code IS NULL
                AND UPPER(dap.legal_entity_country_code) = 'USA'"""
    )
    sess.commit()

    logger.info("Finished FPDS legal entity 9-digit zips without dashes, starting FPDS legal entity 9-digit zips "
                "with dashes")

    # FPDS LE 9-digit dash
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET legal_entity_county_code = zc.county_number,
                legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                THEN dap.legal_entity_county_name
                                                ELSE UPPER(zc.county_name) END
            FROM zip_county AS zc
            WHERE zc.dashed_zip = dap.legal_entity_zip4
                AND dap.legal_entity_county_code IS NULL
                AND UPPER(dap.legal_entity_country_code) = 'USA'"""
    )
    sess.commit()

    logger.info("Finished FPDS legal entity 9-digit zips with dashes, starting FPDS legal entity 5-digit zips")

    # FPDS LE 5-digit
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET legal_entity_county_code = sc.county_number,
                legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                THEN dap.legal_entity_county_name
                                                ELSE UPPER(sc.county_name) END
            FROM single_county AS sc
            WHERE sc.zip5 = LEFT(dap.legal_entity_zip4, 5)
                AND dap.legal_entity_county_code IS NULL
                AND dap.legal_entity_zip4 ~ '^\d{5}(-?\d{4})?$'
                AND UPPER(dap.legal_entity_country_code) = 'USA'"""
    )
    sess.commit()
    logger.info("Finished FPDS legal entity 5-digit zips, FPDS legal entity updates complete.")


def update_fpds_ppop(sess):
    logger.info("Starting FPDS PPOP derivations, starting FPDS PPOP 9-digit zips without dashes")

    # FPDS PPOP 9-digit no dash
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET place_of_perform_county_co = zc.county_number,
                place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                  THEN dap.place_of_perform_county_na
                                                  ELSE UPPER(zc.county_name) END
            FROM zip_county AS zc
            WHERE zc.combined_zip = dap.place_of_performance_zip4a
                AND dap.place_of_perform_county_co IS NULL
                AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
    )
    sess.commit()

    logger.info("Finished FPDS PPOP 9-digit zips without dashes, starting FPDS PPOP 9-digit zips with dashes")

    # FPDS PPOP 9-digit dash
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET place_of_perform_county_co = zc.county_number,
                place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                  THEN dap.place_of_perform_county_na
                                                  ELSE UPPER(zc.county_name) END
            FROM zip_county AS zc
            WHERE zc.dashed_zip = dap.place_of_performance_zip4a
                AND dap.place_of_perform_county_co IS NULL
                AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
    )
    sess.commit()

    logger.info("Finished FPDS PPOP 9-digit zips with dashes, starting FPDS PPOP 5-digit zips")

    # FPDS PPOP 5-digit
    sess.execute(
        """UPDATE detached_award_procurement AS dap
            SET place_of_perform_county_co = sc.county_number,
                place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                  THEN dap.place_of_perform_county_na
                                                  ELSE UPPER(sc.county_name) END
            FROM single_county AS sc
            WHERE sc.zip5 = LEFT(dap.place_of_performance_zip4a, 5)
                AND dap.place_of_perform_county_co IS NULL
                AND dap.place_of_performance_zip4a ~ '^\d{5}(-?\d{4})?$'
                AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
    )
    sess.commit()
    logger.info("Finished FPDS PPOP 5-digit zips, FPDS PPOP updates complete")


def update_fabs_le(sess):
    logger.info("Starting FABS legal entity derivations, starting FABS legal entity 9-digit zips")

    # FABS LE 9-digit
    sess.execute(
        """UPDATE published_fabs AS pf
            SET legal_entity_county_code = zc.county_number,
                legal_entity_county_name = CASE WHEN pf.legal_entity_county_name IS NOT NULL
                                                  THEN pf.legal_entity_county_name
                                                  ELSE zc.county_name END
            FROM zip_county AS zc
            WHERE zc.zip5 = pf.legal_entity_zip5
                AND zc.zip_last4 = pf.legal_entity_zip_last4
                AND pf.legal_entity_county_code IS NULL
                AND UPPER(pf.legal_entity_country_code) = 'USA'
                AND pf.is_active = True"""
    )
    sess.commit()

    logger.info("Finished FABS legal entity 9-digit zips, starting FABS legal entity 5-digit zips")

    # FABS LE 5-digit
    sess.execute(
        """UPDATE published_fabs AS pf
            SET legal_entity_county_code = sc.county_number,
                legal_entity_county_name = CASE WHEN pf.legal_entity_county_name IS NOT NULL
                                                  THEN pf.legal_entity_county_name
                                                  ELSE sc.county_name END
            FROM single_county AS sc
            WHERE sc.zip5 = pf.legal_entity_zip5
                AND pf.legal_entity_county_code IS NULL
                AND UPPER(pf.legal_entity_country_code) = 'USA'
                AND pf.is_active = True"""
    )
    sess.commit()

    logger.info("Finished FABS legal 5-digit zips, FABS legal entity updates complete")


def update_fabs_ppop(sess):
    logger.info("Starting FABS PPOP derivations, starting FABS PPOP 9-digit zips without dashes")

    # FABS PPOP 9-digit no dash
    sess.execute(
        """UPDATE published_fabs AS pf
            SET place_of_perform_county_co = zc.county_number,
                place_of_perform_county_na = CASE WHEN pf.place_of_perform_county_na IS NOT NULL
                                                  THEN pf.place_of_perform_county_na
                                                  ELSE zc.county_name END
            FROM zip_county AS zc
            WHERE zc.combined_zip = pf.place_of_performance_zip4a
                AND pf.place_of_perform_county_co IS NULL
                AND UPPER(pf.place_of_perform_country_c) = 'USA'
                AND pf.is_active = True"""
    )
    sess.commit()

    logger.info("Finished FABS PPOP 9-digit zips without dashes, starting FABS PPOP 9-digit zips with dashes")

    # FABS PPOP 9-digit dash
    sess.execute(
        """UPDATE published_fabs AS pf
            SET place_of_perform_county_co = zc.county_number,
                place_of_perform_county_na = CASE WHEN pf.place_of_perform_county_na IS NOT NULL
                                                  THEN pf.place_of_perform_county_na
                                                  ELSE zc.county_name END
            FROM zip_county AS zc
            WHERE zc.dashed_zip = pf.place_of_performance_zip4a
                AND pf.place_of_perform_county_co IS NULL
                AND UPPER(pf.place_of_perform_country_c) = 'USA'
                AND pf.is_active = True"""
    )
    sess.commit()

    logger.info("Finished FABS PPOP 9-digit zips with dashes, starting FABS PPOP 5-digit zips")

    # FABS PPOP 5-digit
    sess.execute(
        """UPDATE published_fabs AS pf
            SET place_of_perform_county_co = sc.county_number,
                place_of_perform_county_na = CASE WHEN pf.place_of_perform_county_na IS NOT NULL
                                                  THEN pf.place_of_perform_county_na
                                                  ELSE sc.county_name END
            FROM single_county AS sc
            WHERE sc.zip5 = LEFT(pf.place_of_performance_zip4a, 5)
                AND pf.place_of_perform_county_co IS NULL
                AND pf.place_of_performance_zip4a ~ '^\d{5}(-?\d{4})?$'
                AND UPPER(pf.place_of_perform_country_c) = 'USA'
                AND pf.is_active = True"""
    )
    sess.commit()

    logger.info("Finished FABS PPOP 5-digit zips, FABS PPOP updates complete.")


def main():
    sess = GlobalDB.db().session

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-mv', '--matview', help='Create the matviews, make sure they do not already exist',
                        action='store_true')
    parser.add_argument('-dmv', '--delete_matview', help='Delete the matviews', action='store_true')
    parser.add_argument('-fpdsle', '--fpds_le', help='Run FPDS Legal Entity updates', action='store_true')
    parser.add_argument('-fpdsppop', '--fpds_ppop', help='Run FPDS PPOP updates', action='store_true')
    parser.add_argument('-fabsle', '--fabs_le', help='Run FABS Legal Entity updates', action='store_true')
    parser.add_argument('-fabsppop', '--fabs_ppop', help='Run FABS PPOP updates', action='store_true')
    parser.add_argument('-a', '--all', help='Run all updates without creating or deleting matviews',
                        action='store_true')
    parser.add_argument('-am', '--all_matview', help='Run all updates and create and delete matviews',
                        action='store_true')
    args = parser.parse_args()

    logger.info("Starting county code fixes")

    if args.all_matview or args.all:
        if args.all_matview:
            create_matviews(sess)

        update_fpds_le(sess)
        update_fpds_ppop(sess)
        update_fabs_le(sess)
        update_fabs_ppop(sess)

        if args.all_matview:
            delete_matviews(sess)
    else:
        if args.matview:
            create_matviews(sess)
        if args.fpds_le:
            update_fpds_le(sess)
        if args.fpds_ppop:
            update_fpds_ppop(sess)
        if args.fabs_le:
            update_fabs_le(sess)
        if args.fabs_ppop:
            update_fabs_ppop(sess)
    if args.delete_matview:
        delete_matviews(sess)

    logger.info("Completed county code fixes")


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
