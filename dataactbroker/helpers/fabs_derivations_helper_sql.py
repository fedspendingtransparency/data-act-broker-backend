import logging

from datetime import datetime

from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)
from dataactcore.utils.business_categories import derive_fabs_business_categories

logger = logging.getLogger(__name__)


def derive_total_funding_amount(sess, submission_id):
    """ Deriving the total funding amounts

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning total_funding_amount derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance
        SET total_funding_amount = COALESCE(federal_action_obligation, 0) + COALESCE(non_federal_funding_amount, 0)
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed total_funding_amount derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_cfda(sess, submission_id):
    """ Deriving cfda title from cfda number using cfda program table.

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    # TODO: Put the warning back in somehow, maybe do a distinct select of all empty cfda titles
    logger.info({
        'message': 'Beginning cfda_title derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET cfda_title = cfda.program_title
        FROM cfda_program
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND pafa.cfda_number = to_char(cfda.program_number, 'FM00.000');
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed cfda_title derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_awarding_agency_data(sess, submission_id):
    """ Deriving awarding sub tier agency name, awarding agency name, and awarding agency code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning awarding_agency data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving awarding sub tier agency code
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET awarding_sub_tier_agency_c = office.sub_tier_code
        FROM office
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND COALESCE(awarding_sub_tier_agency_c, '') = ''
            AND pafa.awarding_office_code = office.office_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving awarding agency code/name and sub tier name
    query = """
        WITH agency_list AS
            (SELECT (CASE WHEN sta.is_frec
                        THEN frec.frec_code
                        ELSE cgac.cgac_code
                        END) AS agency_code,
                (CASE WHEN sta.is_frec
                    THEN frec.agency_name
                    ELSE cgac.agency_name
                    END) AS agency_name,
                sta.sub_tier_agency_code AS sub_tier_code,
                sta.sub_tier_agency_name AS sub_tier_name
            FROM sub_tier_agency AS sta
                INNER JOIN cgac
                    ON cgac.cgac_id = sta.cgac_id
                INNER JOIN frec
                    ON frec.frec_id = sta.frec_id)
        UPDATE published_award_financial_assistance AS pafa
        SET awarding_agency_code = agency_code,
            awarding_agency_name = agency_name,
            awarding_sub_tier_agency_n = sub_tier_name
        FROM agency_list
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND awarding_sub_tier_agency_c = sub_tier_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed awarding_agency data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_funding_agency_data(sess, submission_id):
    """ Deriving funding sub tier agency name, funding agency name, and funding agency code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning funding_agency data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving funding sub tier agency code
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET funding_sub_tier_agency_co = office.sub_tier_code
        FROM office
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND COALESCE(funding_sub_tier_agency_co, '') = ''
            AND pafa.awarding_office_code = office.office_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving funding agency code/name and sub tier name
    query = """
        WITH agency_list AS
            (SELECT (CASE WHEN sta.is_frec
                        THEN frec.frec_code
                        ELSE cgac.cgac_code
                        END) AS agency_code,
                (CASE WHEN sta.is_frec
                    THEN frec.agency_name
                    ELSE cgac.agency_name
                    END) AS agency_name,
                sta.sub_tier_agency_code AS sub_tier_code,
                sta.sub_tier_agency_name AS sub_tier_name
            FROM sub_tier_agency AS sta
                INNER JOIN cgac
                    ON cgac.cgac_id = sta.cgac_id
                INNER JOIN frec
                    ON frec.frec_id = sta.frec_id)
        UPDATE published_award_financial_assistance AS pafa
        SET funding_agency_code = agency_code,
            funding_agency_name = agency_name,
            funding_sub_tier_agency_na = sub_tier_name
        FROM agency_list
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND funding_sub_tier_agency_co = sub_tier_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed funding_agency data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_ppop_state(sess, submission_id):
    """ Deriving ppop code and ppop state name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning place of performance state derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving office codes for record type not 1
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_perfor_state_code = CASE WHEN UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]'
                                              THEN state_code
                                              ELSE NULL
                                         END,
            place_of_perform_state_nam = CASE WHEN place_of_performance_code = '00*****'
                                              THEN 'Multi-state'
                                              ELSE state_name
                                         END
        FROM states
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND (UPPER(SUBSTRING(place_of_performance_code, 1, 2)) = state_code
                    OR place_of_performance_code = '00*****');
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed place of performance state derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def split_ppop_zip(sess, submission_id):
    """ Splitting ppop zip code into 5 and 4 digit codes for ease of website access

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning place of performance zip5 and zip last4 derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_performance_zip5 = SUBSTRING(place_of_performance_zip4a, 1, 5),
            place_of_perform_zip_last4 = CASE WHEN LENGTH(place_of_performance_zip4a) = 5
                                              THEN NULL
                                              ELSE RIGHT(place_of_performance_zip4a, 4)
                                         END
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip4a ~ '^\d\d\d\d\d(-?\d\d\d\d)?$';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed place of performance zip5 and zip last4 derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_ppop_location_data(sess, submission_id):
    """ Deriving place of performance location values from zip4

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning place of performance location derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving congressional and county info for records with a 9 digit zip
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_performance_congr = CASE WHEN place_of_performance_congr IS NULL
                                              THEN congressional_district_no
                                              ELSE place_of_performance_congr
                                         END,
            place_of_perform_county_co = county_number
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_perform_zip_last4 IS NOT NULL
            AND place_of_perform_zip_last4 = zip_last4
            AND place_of_performance_zip5 = zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving congressional info for multi-district zips
    query = """
        WITH all_sub_zips AS
            (SELECT DISTINCT place_of_performance_zip5
            FROM published_award_financial_assistance
            WHERE submission_id = {submission_id}
                AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
                AND place_of_performance_congr IS NULL),
        congr_dist AS
            (SELECT COUNT(DISTINCT congressional_district_no) AS cd_count, zip5
            FROM zips
            WHERE EXISTS (
                SELECT 1
                FROM all_sub_zips AS asz
                WHERE zips.zip5 = asz.place_of_performance_zip5)
            GROUP BY zip5)
        UPDATE published_award_financial_assistance
        SET place_of_performance_congr = CASE WHEN cd_count > 1
                                              THEN '90'
                                         END
        FROM congr_dist
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_congr IS NULL
            AND zip5 = place_of_performance_zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving congressional info for remaining blanks (with zip code)
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_performance_congr = congressional_district_no
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip5 = zip5
            AND place_of_performance_congr IS NULL;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county code info for remaining blanks (with zip code)
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_perform_county_co = county_number
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip5 = zip5
            AND place_of_perform_county_co IS NULL;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving city info for transactions with zips
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_performance_city = city_name
        FROM zip_city
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip5 IS NOT NULL
            AND zip_city.zip_code = place_of_performance_zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county code info for transactions with ppop code XX**###
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_perform_county_co = RIGHT(place_of_performance_code, 3)
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip5 IS NULL
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$';
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county/city info for transactions with ppop code XX#####
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_perform_county_co = county_number,
            place_of_perform_county_na = county_name,
            place_of_performance_city = feature_name
        FROM city_code AS cc
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_performance_zip5 IS NULL
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d\d$'
            AND cc.city_code = RIGHT(place_of_performance_code, 5)
            AND cc.state_code = place_of_perfor_state_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving remaining county names
    query = """
        UPDATE published_award_financial_assistance
        SET place_of_perform_county_na = county_name
        FROM county_code AS cc
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND place_of_perform_county_na IS NULL
            AND UPPER(place_of_perform_county_co) IS NOT NULL
            AND cc.county_number = place_of_perform_county_co
            AND cc.state_code = place_of_perfor_state_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed place of performance location derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_ppop_scope(sess, submission_id):
    """ Deriving place of performance scope values from zip4 and place of performance code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning place of performance scope derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # When zip is not null
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_performance_scope = CASE WHEN UPPER(place_of_performance_zip4a) = 'CITY-WIDE'
                                              THEN 'City-wide'
                                              WHEN place_of_performance_zip4a ~ '^\d\d\d\d\d(\-?\d\d\d\d)?$'
                                              THEN 'Single ZIP Code'
                                              ELSE NULL
                                         END
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND COALESCE(place_of_performance_zip4a, '') <> '';
    """
    sess.execute(query.format(submission_id=submission_id))

    # When zip is null
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_performance_scope = CASE WHEN UPPER(place_of_performance_code) ~ '^[A-Z]{2}\d{4}[\dR]$'
                                              THEN 'City-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^[A-Z]{2}\*\*\d{3}$'
                                              THEN 'County-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^[A-Z]{2}\*{5}$'
                                              THEN 'State-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^00\*{5}$'
                                              THEN 'Multi-state'
                                              WHEN UPPER(place_of_performance_code) ~ '^00FORGN$'
                                              THEN 'Foreign'
                                              ELSE NULL
                                         END
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND COALESCE(place_of_performance_zip4a, '') = '';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed place of performance scope derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_le_location_data(sess, submission_id):
    """ Deriving place of performance location values

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning legal entity location derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving congressional, county, and state info for records with a 9 digit zip
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_congressional = CASE WHEN legal_entity_congressional IS NULL
                                              THEN congressional_district_no
                                              ELSE legal_entity_congressional
                                         END,
            legal_entity_county_code = county_number,
            legal_entity_state_code = state_abbreviation
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip_last4 IS NOT NULL
            AND legal_entity_zip_last4 = zip_last4
            AND legal_entity_zip5 = zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving congressional info for multi-district zips
    query = """
        WITH all_sub_zips AS
            (SELECT DISTINCT legal_entity_zip5
            FROM published_award_financial_assistance
            WHERE submission_id = {0}
                AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
                AND legal_entity_congressional IS NULL),
        congr_dist AS
            (SELECT COUNT(DISTINCT congressional_district_no) AS cd_count, zip5
            FROM zips
            WHERE EXISTS (
                SELECT 1
                FROM all_sub_zips AS asz
                WHERE zips.zip5 = asz.legal_entity_zip5)
            GROUP BY zip5)
        UPDATE published_award_financial_assistance
        SET legal_entity_congressional = CASE WHEN cd_count > 1
                                              THEN '90'
                                         END
        FROM congr_dist
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_congressional IS NULL
            AND zip5 = legal_entity_zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving congressional info for remaining blanks (with zip code)
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_congressional = congressional_district_no
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip5 = zip5
            AND legal_entity_congressional IS NULL;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county and state code info for remaining blanks (with zip code)
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_county_code = county_number,
            legal_entity_state_code = state_abbreviation 
        FROM zips
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip5 = zip5
            AND legal_entity_county_code IS NULL;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county names for records with zips (type 2 and 3)
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_county_name = county_name
        FROM county_code AS cc
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip5 IS NOT NULL
            AND cc.county_number = legal_entity_county_code
            AND cc.state_code = legal_entity_state_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving state names for records with zips (type 2 and 3)
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_state_name = state_name
        FROM states
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip5 IS NOT NULL
            AND states.state_code = legal_entity_state_code;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving city info for records with zips (type 2 and 3)
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_city_name = city_name
        FROM zip_city
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND legal_entity_zip5 IS NOT NULL
            AND zip_city.zip_code = legal_entity_zip5;
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county, state, and congressional info for county format ppop codes in record type 1
    query = """
        UPDATE published_award_financial_assistance
        SET legal_entity_county_code = place_of_perform_county_co,
            legal_entity_county_name = place_of_perform_county_na,
            legal_entity_state_code = place_of_perfor_state_code,
            legal_entity_state_name = place_of_perform_state_nam,
            legal_entity_congressional = place_of_performance_congr
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND record_type = 1
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$';
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving county, state, and congressional info for state format ppop codes in record type 1
    query = """
        UPDATE published_award_financial_assistance
            legal_entity_state_code = place_of_perfor_state_code,
            legal_entity_state_name = place_of_perform_state_nam,
            legal_entity_congressional = place_of_performance_congr
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND record_type = 1
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\*\*\*$';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed legal entity location derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_office_data(sess, submission_id):
    """ Deriving office data

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning office data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving office codes for record type not 1
    query = """
        WITH awards AS
            (SELECT DISTINCT UPPER(fain) AS upper_fain,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance
            WHERE submission_id = {submission_id}
                AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
                AND record_type <> '1'),
        min_date AS
            (SELECT CAST(MIN(action_date) AS DATE) AS min_date,
                UPPER(fain) AS upper_fain,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type <> '1'
                AND submission_id <> {submission_id}
                AND EXISTS (
                    SELECT 1
                    FROM awards
                    WHERE upper_fain = UPPER(fain)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                )
            GROUP BY UPPER(fain), UPPER(awarding_sub_tier_agency_c)),
        office_info AS
            (SELECT UPPER(awarding_office_code) AS awarding_office_code,
                UPPER(funding_office_code) AS funding_office_code,
                award_modification_amendme,
                UPPER(fain) AS upper_fain,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type <> '1'
                AND EXISTS (
                    SELECT 1
                    FROM min_date AS md
                    WHERE upper_fain = UPPER(fain)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                        AND CAST(pafa.action_date AS DATE) = min_date
                )),
        filtered_offices AS
            (SELECT award_modification_amendme,
                upper_fain,
                upper_sub_tier,
                aw_office.office_code AS awarding_office_code,
                fund_office.office_code AS funding_office_code
            FROM office_info AS oi
            LEFT JOIN office AS aw_office
                ON aw_office.office_code = oi.awarding_office_code
                AND aw_office.financial_assistance_awards_office IS TRUE
            LEFT JOIN office AS fund_office
                ON fund_office.office_code = oi.funding_office_code
                AND (fund_office.contract_funding_office IS TRUE
                    OR fund_office.financial_assistance_funding_office IS TRUE))
        UPDATE published_award_financial_assistance AS pafa
        SET awarding_office_code = CASE WHEN pafa.awarding_office_code IS NULL
                                        THEN fo.awarding_office_code
                                        ELSE pafa.awarding_office_code
                                   END,
            funding_office_code = CASE WHEN pafa.funding_office_code IS NULL
                                       THEN fo.funding_office_code
                                       ELSE pafa.funding_office_code
                                  END
        FROM filtered_offices AS fo
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND pafa.award_modification_amendme <> fo.award_modification_amendme
            AND upper_fain = fain
            AND upper_sub_tier = awarding_sub_tier_agency_c
            AND record_type <> '1';
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving office codes for record type 1
    query = """
        WITH awards AS
            (SELECT DISTINCT UPPER(uri) AS upper_uri,
            UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance
            WHERE submission_id = {submission_id}
                AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
                AND record_type = '1'),
        min_date AS
            (SELECT CAST(MIN(action_date) AS DATE) AS min_date,
            UPPER(uri) AS upper_uri, UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type = '1'
                AND submission_id <> {submission_id}
                AND EXISTS (
                    SELECT 1
                    FROM awards
                    WHERE upper_uri = UPPER(uri)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                )
            GROUP BY UPPER(uri), UPPER(awarding_sub_tier_agency_c)),
        office_info AS
            (SELECT UPPER(awarding_office_code) AS awarding_office_code,
                UPPER(funding_office_code) AS funding_office_code,
                award_modification_amendme,
                UPPER(uri) AS upper_uri,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type = '1'
                AND EXISTS (
                    SELECT 1
                    FROM min_date AS md
                    WHERE upper_uri = UPPER(uri)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                        AND CAST(pafa.action_date AS DATE) = min_date
                )),
        filtered_offices AS
            (SELECT award_modification_amendme,
                upper_uri,
                upper_sub_tier,
                aw_office.office_code AS awarding_office_code,
                fund_office.office_code AS funding_office_code
            FROM office_info AS oi
            LEFT JOIN office AS aw_office
                ON aw_office.office_code = oi.awarding_office_code
                AND aw_office.financial_assistance_awards_office IS TRUE
            LEFT JOIN office AS fund_office
                ON fund_office.office_code = oi.funding_office_code
                AND (fund_office.contract_funding_office IS TRUE
                    OR fund_office.financial_assistance_funding_office IS TRUE))
        UPDATE published_award_financial_assistance AS pafa
        SET awarding_office_code = CASE WHEN pafa.awarding_office_code IS NULL
                                        THEN fo.awarding_office_code
                                        ELSE pafa.awarding_office_code
                                   END,
            funding_office_code = CASE WHEN pafa.funding_office_code IS NULL
                                       THEN fo.funding_office_code
                                       ELSE pafa.funding_office_code
                                  END
        FROM filtered_offices AS fo
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND pafa.award_modification_amendme <> fo.award_modification_amendme
            AND upper_uri = uri
            AND upper_sub_tier = awarding_sub_tier_agency_c
            AND record_type = '1';
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving awarding office name
    query = """
        UPDATE published_award_financial_assistance
        SET awarding_office_name = office_name
        FROM office
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND office_code = UPPER(awarding_office_code);
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving funding office name
    query = """
        UPDATE published_award_financial_assistance
        SET funding_office_name = office_name
        FROM office
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND office_code = UPPER(funding_office_code);
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed office data derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_le_city_code(sess, submission_id):
    """ Deriving legal entity city code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning legal entity city code derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET legal_entity_city_code = city_code
        FROM city_code
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND UPPER(TRIM(legal_entity_city_name)) = UPPER(feature_name)
            AND UPPER(TRIM(legal_entity_state_code)) = UPPER(state_code);
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed legal entity city code derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_ppop_country_name(sess, submission_id):
    """ Deriving place of performance country name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning place of performance country name derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_perform_country_n = country_name
        FROM country_code
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND country_code.country_code = place_of_perform_country_c;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed place of performance country name derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_le_country_name(sess, submission_id):
    """ Deriving legal entity country name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning legal entity country name derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_perform_country_n = country_name
        FROM country_code
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND country_code.country_code = place_of_perform_country_c;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed legal entity country name derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_pii_redacted_ppop_data(sess, submission_id):
    """ Deriving ppop code and location data for PII-redacted records

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning PII redacted information derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Deriving information for USA records
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_performance_code = CASE WHEN legal_entity_state_code IS NOT NULL
                                            THEN CASE WHEN legal_entity_city_code IS NOT NULL
                                                    THEN UPPER(legal_entity_state_code) || legal_entity_city_code
                                                    ELSE UPPER(legal_entity_state_code) || '00000'
                                                    END
                                            ELSE NULL
                                            END,
            place_of_perform_country_c = legal_entity_country_code,
            place_of_perform_country_n = legal_entity_country_name,
            place_of_performance_city = legal_entity_city_name,
            place_of_perform_county_co = legal_entity_county_code,
            place_of_perform_county_na = legal_entity_county_name,
            place_of_perfor_state_code = legal_entity_state_code,
            place_of_perform_state_nam = legal_entity_state_name,
            place_of_performance_zip4a = legal_entity_zip5,
            place_of_performance_zip5 = legal_entity_zip5,
            place_of_performance_congr = legal_entity_congressional
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND record_type = 3
            AND UPPER(legal_entity_country_code) = 'USA';
    """
    sess.execute(query.format(submission_id=submission_id))

    # Deriving information for non-USA records
    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET place_of_performance_code = '00FORGN',
            place_of_perform_country_c = legal_entity_country_code,
            place_of_perform_country_n = legal_entity_country_name,
            place_of_performance_city = legal_entity_foreign_city,
            place_of_performance_forei = legal_entity_foreign_city
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND record_type = 3
            AND UPPER(legal_entity_country_code) <> 'USA';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed PII redacted information derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_parent_duns(sess, submission_id):
    """ Deriving parent DUNS name and number from SAM API

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning parent DUNS derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET ultimate_parent_legal_enti = duns.ultimate_parent_legal_enti,
            ultimate_parent_unique_ide = duns.ultimate_parent_unique_ide
        FROM duns
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND pafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND (duns.ultimate_parent_legal_enti IS NOT NULL
                OR duns.ultimate_parent_unique_ide IS NOT NULL);
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed parent DUNS derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_executive_compensation(sess, submission_id):
    """ Deriving Executive Compensation information from DUNS.

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning executive compensation derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET high_comp_officer1_full_na = duns.high_comp_officer1_full_na,
            high_comp_officer1_amount = duns.high_comp_officer1_amount,
            high_comp_officer2_full_na = duns.high_comp_officer2_full_na,
            high_comp_officer2_amount = duns.high_comp_officer2_amount,
            high_comp_officer3_full_na = duns.high_comp_officer3_full_na,
            high_comp_officer3_amount = duns.high_comp_officer3_amount,
            high_comp_officer4_full_na = duns.high_comp_officer4_full_na,
            high_comp_officer4_amount = duns.high_comp_officer4_amount,
            high_comp_officer5_full_na = duns.high_comp_officer5_full_na,
            high_comp_officer5_amount = duns.high_comp_officer5_amount
        FROM duns
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND pafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND high_comp_officer1_full_na IS NOT NULL;
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed executive compensation derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def derive_labels(sess, submission_id):
    """ Deriving labels for codes entered by the user

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning label derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    # Action type description derivation
    action_type_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in ACTION_TYPE_DICT.items())
    query = """
        WITH action_type_desc AS
            (SELECT *
            FROM (VALUES ({action_types})) as action_type_desc(letter, description))
        UPDATE published_award_financial_assistance AS pafa
        SET action_type_description = description
        FROM action_type_desc AS atd
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND atd.letter = UPPER(pafa.action_type);
    """
    sess.execute(query.format(submission_id=submission_id, action_types=action_type_values))

    # Assistance type description derivation
    assistance_type_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in ASSISTANCE_TYPE_DICT.items())
    query = """
        WITH assistance_type_description AS
            (SELECT *
            FROM (VALUES ({assistance_types})) as assistance_type_description(letter, description))
        UPDATE published_award_financial_assistance AS pafa
        SET assistance_type_desc = description
        FROM assistance_type_description AS atd
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND atd.letter = UPPER(pafa.assistance_type);
    """
    sess.execute(query.format(submission_id=submission_id, assistance_types=assistance_type_values))

    # CorrectionDeleteIndicator description derivation
    cdi_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in CORRECTION_DELETE_IND_DICT.items())
    query = """
        WITH cdi_desc AS
            (SELECT *
            FROM (VALUES ({cdi_types})) as cdi_desc(letter, description))
        UPDATE published_award_financial_assistance AS pafa
        SET correction_delete_ind_desc = description
        FROM cdi_desc
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND cdi_desc.letter = UPPER(pafa.correction_delete_indicatr);
    """
    sess.execute(query.format(submission_id=submission_id, cdi_types=cdi_values))

    # Record Type description derivation
    record_type_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in RECORD_TYPE_DICT.items())
    query = """
        WITH record_type_desc AS
            (SELECT *
            FROM (VALUES ({record_types})) as record_type_desc(letter, description))
        UPDATE published_award_financial_assistance AS pafa
        SET record_type_description = description
        FROM record_type_desc AS rtd
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND rtd.letter = pafa.record_type;
    """
    sess.execute(query.format(submission_id=submission_id, record_types=record_type_values))

    # Business Funds Indicator description derivation
    business_funds_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in BUSINESS_FUNDS_IND_DICT.items())
    query = """
        WITH business_funds_ind_description AS
            (SELECT *
            FROM (VALUES ({business_funds_ind})) as business_funds_ind_description(letter, description))
        UPDATE published_award_financial_assistance AS pafa
        SET business_funds_ind_desc = description
        FROM business_funds_ind_description AS bfid
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            AND bfid.letter = UPPER(pafa.business_funds_indicator);
    """
    sess.execute(query.format(submission_id=submission_id, business_funds_ind=business_funds_values))

    # Business types description derivation
    business_types_values = '), ('.join('{}, {}'.format(name, desc) for name, desc in BUSINESS_TYPE_DICT.items())
    query = """
        WITH business_type_desc AS
            (SELECT *
            FROM(VALUES ({business_types})) as business_type_desc(letter, description)),
        aggregated_business_types AS
            (SELECT published_award_financial_assistance_id,
                string_agg(btd.description, ';' order by ordinality) AS aggregated
            FROM published_award_financial_assistance AS pafa,
                unnest(string_to_array(pafa.business_types, NULL)) WITH ORDINALITY AS u(business_type_id, ordinality)
            LEFT JOIN business_type_desc AS btd
                ON btd.letter = UPPER(business_type_id)
            WHERE submission_id = {submission_id}
                AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
            GROUP BY published_award_financial_assistance_id)
        UPDATE published_award_financial_assistance AS pafa
        SET business_types_desc = abt.aggregated
        FROM aggregated_business_types AS abt
        WHERE
            abt.published_award_financial_assistance_id = pafa.published_award_financial_assistance_id;
    """
    sess.execute(query.format(submission_id=submission_id, business_types=business_types_values))

    logger.info({
        'message': 'Completed label derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def set_active(sess, submission_id):
    """ Setting active

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning active setting',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET is_active = True
        WHERE submission_id = {submission_id}
            AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed active derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def set_modified_at(sess, submission_id):
    """ Setting modified_at date

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    logger.info({
        'message': 'Beginning active setting',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })

    query = """
        UPDATE published_award_financial_assistance AS pafa
        SET modified_at = NOW()
        WHERE submission_id = {submission_id};
    """
    sess.execute(query.format(submission_id=submission_id))

    logger.info({
        'message': 'Completed active derivation',
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    })


def fabs_derivations(sess, submission_id):
    """ Performs derivations related to publishing a FABS submission

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    # TODO: Decide if we want to include the job in the logs
    # TODO: Decide if we want to log each SQL query (including start/end or just the end) or just each function
    # TODO: Decide if we want to include duration in our logs
    # TODO: ADD INDEXES,
    #   known required ones:
    #       multicolumn(UPPER(fain), UPPER(awarding_sub_tier_agency_c)))
    #       place_of_performance_zip5
    #       place_of_perform_zip_last4
    #       place_of_performance_congr
    #       place_of_perform_county_co
    #       MAYBE: place_of_perfor_state_code
    derive_total_funding_amount(sess, submission_id)

    derive_cfda(sess, submission_id)

    derive_awarding_agency_data(sess, submission_id)

    derive_funding_agency_data(sess, submission_id)

    derive_ppop_state(sess, submission_id)

    split_ppop_zip(sess, submission_id)

    derive_ppop_location_data(sess, submission_id)

    derive_le_location_data(sess, submission_id)

    derive_office_data(sess, submission_id)

    derive_le_city_code(sess, submission_id)

    derive_ppop_country_name(sess, submission_id)

    derive_le_country_name(sess, submission_id)

    derive_pii_redacted_ppop_data(sess, submission_id)

    derive_parent_duns(sess, submission_id)

    derive_executive_compensation(sess, submission_id)

    derive_labels(sess, submission_id)

    derive_ppop_scope(sess, submission_id)

    derive_fabs_business_categories(sess, submission_id)

    set_active(sess, submission_id)

    set_modified_at(sess, submission_id)
