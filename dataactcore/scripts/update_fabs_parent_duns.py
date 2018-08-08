import argparse
import logging
import time

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app

from dataactcore.models.domainModels import HistoricParentDUNS
from dataactcore.utils.parentDuns import sam_config_is_valid, get_name_from_sam, update_missing_parent_names

logger = logging.getLogger(__name__)

FABS_PARENT_DUNS_SQL_MATCH = """
    WITH joined_historical_fabs AS (
        SELECT
            hfabs.published_award_financial_assistance_id AS "fabs_id",
            hpd.ultimate_parent_unique_ide AS "parent_duns",
            hpd.ultimate_parent_legal_enti AS "parent_name"
        FROM published_award_financial_assistance hfabs
        JOIN historic_parent_duns hpd ON (
            hfabs.awardee_or_recipient_uniqu=hpd.awardee_or_recipient_uniqu AND
            EXTRACT(YEAR FROM cast_as_date(hfabs.action_date))=hpd.year
        )
        WHERE (
            hfabs.ultimate_parent_unique_ide IS NULL
        )
    )
    UPDATE published_award_financial_assistance AS fabs
    SET
       ultimate_parent_unique_ide = joined_historical_fabs.parent_duns,
       ultimate_parent_legal_enti = joined_historical_fabs.parent_name
    FROM joined_historical_fabs
    WHERE
       joined_historical_fabs.fabs_id = fabs.published_award_financial_assistance_id
"""

CREATE_TEMP_MIN_YEARS_MATVIEW = """
    CREATE MATERIALIZED VIEW tmp_matview_min_years
    AS (
        SELECT awardee_or_recipient_uniqu, MIN(year) as "min_year"
        FROM historic_parent_duns
        GROUP BY awardee_or_recipient_uniqu
    );

    CREATE INDEX min_years_min_year_idx ON tmp_matview_min_years (min_year);
    CREATE INDEX min_years_awardee_or_recipient_uniqu_idx ON tmp_matview_min_years (awardee_or_recipient_uniqu);
"""

FABS_PARENT_DUNS_SQL_EARLIEST = """
    WITH joined_historical_fabs AS (
        SELECT
            hfabs.published_award_financial_assistance_id AS "fabs_id",
            hpd.ultimate_parent_unique_ide AS "parent_duns",
            hpd.ultimate_parent_legal_enti AS "parent_name"
        FROM published_award_financial_assistance hfabs
        JOIN historic_parent_duns hpd ON (
            hfabs.awardee_or_recipient_uniqu=hpd.awardee_or_recipient_uniqu
        )
        JOIN tmp_matview_min_years min_years ON (
            hfabs.awardee_or_recipient_uniqu = min_years.awardee_or_recipient_uniqu
        )
        WHERE (
            hpd.year = min_years.min_year AND
            hfabs.ultimate_parent_unique_ide IS NULL
        )
    )
    UPDATE published_award_financial_assistance AS fabs
    SET
       ultimate_parent_unique_ide = joined_historical_fabs.parent_duns,
       ultimate_parent_legal_enti = joined_historical_fabs.parent_name
    FROM joined_historical_fabs
    WHERE
       joined_historical_fabs.fabs_id = fabs.published_award_financial_assistance_id;
"""

DROP_TEMP_MIN_YEARS_MATVIEW = """
    DROP INDEX min_years_min_year_idx;
    DROP INDEX min_years_awardee_or_recipient_uniqu_idx;
    DROP MATERIALIZED VIEW tmp_matview_min_years;
"""


def update_historic_parent_names_duns_service(sess):
    start = time.time()
    logger.info("Gathering historical parent duns names via SAM service")
    client = sam_config_is_valid()
    hist_duns = sess.query(HistoricParentDUNS).filter(HistoricParentDUNS.ultimate_parent_unique_ide.isnot(None),
                                                      HistoricParentDUNS.ultimate_parent_legal_enti.is_(None))
    duns_count = hist_duns.count()
    if not duns_count:
        logger.info("Historical Parent DUNS table already has the names updated.")
        return
    block_size = 100
    batch = 0
    batches = duns_count // block_size
    logger.info("Updating historical parent duns names")
    all_models = []
    while batch <= batches:
        batch_start = batch * block_size
        sliced_duns = hist_duns.order_by(HistoricParentDUNS.duns_id).slice(batch_start, batch_start + block_size)
        models = {}
        for row in sliced_duns:
            if row.ultimate_parent_unique_ide not in models:
                models[row.ultimate_parent_unique_ide] = [row]
            else:
                models[row.ultimate_parent_unique_ide].append(row)
        sliced_duns_list = [str(duns) for duns in list(models.keys())]
        if sliced_duns_list:
            duns_parent_df = get_name_from_sam(client, sliced_duns_list)
            duns_parent_df = duns_parent_df.rename(columns={'awardee_or_recipient_uniqu': 'ultimate_parent_unique_ide',
                                                            'legal_business_name': 'ultimate_parent_legal_enti'})
            for _, row in duns_parent_df.iterrows():
                parent_duns = row['ultimate_parent_unique_ide']
                if parent_duns not in models:
                    models[parent_duns] = [HistoricParentDUNS()]
                for field, value in row.items():
                    for model in models[parent_duns]:
                        setattr(model, field, value)
            all_models.extend([model for model_list in models.values() for model in model_list])
        batch += 1
    sess.add_all(all_models)
    sess.commit()
    logger.info("Updated historical Parent DUNS names through SAM service, took {} seconds".format(time.time()-start))


def update_historic_parent_names(sess):
    update_historic_parent_names_duns_service(sess)

    start = time.time()
    logger.info("Updating historical Parent DUNS names using internal data")
    while update_missing_parent_names(sess, table=HistoricParentDUNS):
        continue
    logger.info("Updated historical Parent DUNS names using internal data, took {} seconds".format(time.time()-start))

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        parser = argparse.ArgumentParser(description='Update parent duns columns in FABS table')
        args = parser.parse_args()
        sess = GlobalDB.db().session

        update_historic_parent_names(sess)

        start = time.time()
        logger.info("Updating FABS with action dates matching the years within the parent duns")
        sess.execute(FABS_PARENT_DUNS_SQL_MATCH)
        sess.commit()
        logger.info("Updated FABS with action dates matching the years within the parent duns, took {} seconds"
                    .format(time.time()-start))

        start = time.time()
        logger.info("Updating FABS with action dates not matching the parent duns, using the earliest match")
        sess.execute(CREATE_TEMP_MIN_YEARS_MATVIEW)
        sess.commit()
        sess.execute(FABS_PARENT_DUNS_SQL_EARLIEST)
        sess.commit()
        sess.execute(DROP_TEMP_MIN_YEARS_MATVIEW)
        sess.commit()
        logger.info("Updated FABS with action dates not matching the parent duns, using the earliest match, "
                    "took {} seconds".format(time.time()-start))

        sess.close()
        logger.info("Historical parent DUNS FABS updates complete.")
