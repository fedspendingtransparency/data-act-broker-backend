import os
import re
import logging
import boto
import urllib.request
import zipfile
import numpy as np
import sys
import pandas as pd
from sqlalchemy import func

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance
from dataactcore.models.domainModels import SubTierAgency, CountyCode, States, Zips, ZipCity, CityCode
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)

def update_funding(sess):
    logger.info('updating funding')
    invalid = sess.execute("select * from detached_award_procurement where funding_agency_code='999'")
    invalid_count = len(invalid.fetchall())
    logger.info("{} invalid rows found")
    res = sess.execute(
        "UPDATE detached_award_procurement " +
        "set funding_agency_code = agency.agency_code, " +
        "funding_agency_name = agency.agency_name " +
        "from ( " +
        "SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) " +
        "end agency_name, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) " +
        "end agency_code " +
        "from sub_tier_agency sub " +
        "INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id " +
        "INNER JOIN frec ON frec.frec_id = sub.frec_id " +
        ") agency " +
        "where detached_award_procurement.funding_agency_code = '999' " +
        "and detached_award_procurement.funding_sub_tier_agency_co = agency.sub_tier_agency_code "
    )
    sess.commit()

def update_awarding(sess):
    logger.info('updating awarding')
    res = sess.execute(
        "UPDATE detached_award_procurement " +
        "set awarding_agency_code = agency.agency_code, " +
        "awarding_agency_name = agency.agency_name " +
        "from ( " +
        "SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) " +
        "end agency_name, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) " +
        "end agency_code " +
        "from sub_tier_agency sub " +
        "INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id " +
        "INNER JOIN frec ON frec.frec_id = sub.frec_id " +
        ") agency " +
        "where detached_award_procurement.awarding_agency_code = '999' " +
        "and detached_award_procurement.awarding_sub_tier_agency_c = agency.sub_tier_agency_code "
    )
    sess.commit()

def main():
    sess = GlobalDB.db().session

    update_funding(sess)
    update_awarding(sess)
    logger.info("Procurement Update Complete")

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
