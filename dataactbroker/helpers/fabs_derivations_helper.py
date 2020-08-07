import logging

from datetime import datetime

from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)
from dataactcore.utils.business_categories import derive_fabs_business_categories

logger = logging.getLogger(__name__)


def log_derivation(message, submission_id, start_time=None):
    """ Just logging the time taken to run whatever derivation is being run.

        Args:
            message: the message to log
            submission_id: the ID of the submission
            start_time: If provided, use it to calculate the duration.
    """
    log_message = {
        'message': message,
        'message_type': 'BrokerDebug',
        'submission_id': submission_id
    }

    if start_time:
        log_message['duration'] = (datetime.now() - start_time).total_seconds()
    logger.info(log_message)


def derive_total_funding_amount(sess, submission_id):
    """ Deriving the total funding amounts

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning total_funding_amount derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id}
        SET total_funding_amount = COALESCE(federal_action_obligation, 0) + COALESCE(non_federal_funding_amount, 0);
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed total_funding_amount derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_cfda(sess, submission_id):
    """ Deriving cfda title from cfda number using cfda program table.

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    # TODO: Put the warning back in somehow, maybe do a distinct select of all empty cfda titles
    start_time = datetime.now()
    log_derivation('Beginning cfda_title derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET cfda_title = cfda.program_title
        FROM cfda_program AS cfda
        WHERE pafa.cfda_number = to_char(cfda.program_number, 'FM00.000');
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed cfda_title derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_awarding_agency_data(sess, submission_id):
    """ Deriving awarding sub tier agency name, awarding agency name, and awarding agency code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning awarding_agency data derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning awarding sub tier code derivation', submission_id)
    # Deriving awarding sub tier agency code
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET awarding_sub_tier_agency_c = office.sub_tier_code
        FROM office
        WHERE UPPER(COALESCE(awarding_sub_tier_agency_c, '')) = ''
            AND UPPER(pafa.awarding_office_code) = office.office_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed sub tier code derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning awarding agency info derivation', submission_id)
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
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET awarding_agency_code = agency_code,
            awarding_agency_name = agency_name,
            awarding_sub_tier_agency_n = sub_tier_name
        FROM agency_list
        WHERE UPPER(awarding_sub_tier_agency_c) = sub_tier_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed awarding agency info derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed awarding_agency data derivation', submission_id, start_time)


def derive_funding_agency_data(sess, submission_id):
    """ Deriving funding sub tier agency name, funding agency name, and funding agency code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning funding_agency data derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning funding sub tier code derivation', submission_id)
    # Deriving funding sub tier agency code
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET funding_sub_tier_agency_co = office.sub_tier_code
        FROM office
        WHERE UPPER(COALESCE(funding_sub_tier_agency_co, '')) = ''
            AND UPPER(pafa.funding_office_code) = office.office_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed funding sub tier code derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning funding agency info derivation', submission_id)
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
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET funding_agency_code = agency_code,
            funding_agency_name = agency_name,
            funding_sub_tier_agency_na = sub_tier_name
        FROM agency_list
        WHERE UPPER(funding_sub_tier_agency_co) = sub_tier_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed funding agency info derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed funding_agency data derivation', submission_id, start_time)


def derive_ppop_state(sess, submission_id):
    """ Deriving ppop code and ppop state name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning place of performance state derivation', submission_id)

    # Deriving office codes for record type not 1
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_perfor_state_code = CASE WHEN UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]'
                                              THEN state_code
                                              ELSE NULL
                                         END,
            place_of_perform_state_nam = CASE WHEN place_of_performance_code = '00*****'
                                              THEN 'Multi-state'
                                              ELSE state_name
                                         END
        FROM states
        WHERE UPPER(SUBSTRING(place_of_performance_code, 1, 2)) = state_code
            OR place_of_performance_code = '00*****';
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed place of performance state derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def split_ppop_zip(sess, submission_id):
    """ Splitting ppop zip code into 5 and 4 digit codes for ease of website access

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning place of performance zip5 and zip last4 derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_performance_zip5 = SUBSTRING(place_of_performance_zip4a, 1, 5),
            place_of_perform_zip_last4 = CASE WHEN LENGTH(place_of_performance_zip4a) = 5
                                              THEN NULL
                                              ELSE RIGHT(place_of_performance_zip4a, 4)
                                         END
        WHERE place_of_performance_zip4a ~ '^\d\d\d\d\d(-?\d\d\d\d)?$';
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed place of performance zip5 and zip last4 derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_ppop_location_data(sess, submission_id):
    """ Deriving place of performance location values from zip4

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning place of performance location derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning ppop congr/county info for 9 digit zips derivation', submission_id)
    # Deriving congressional and county info for records with a 9 digit zip
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_performance_congr = CASE WHEN place_of_performance_congr IS NULL
                                              THEN congressional_district_no
                                              ELSE place_of_performance_congr
                                         END,
            place_of_perform_county_co = county_number
        FROM zips
        WHERE place_of_perform_zip_last4 IS NOT NULL
            AND place_of_perform_zip_last4 = zip_last4
            AND place_of_performance_zip5 = zip5;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop congr/county info for 9 digit zips derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop congr info for 5-digit zip derivation', submission_id)
    # Deriving congressional info for remaining blanks (with zip code)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_performance_congr = congressional_district_no
        FROM zips_grouped
        WHERE place_of_performance_zip5 = zip5
            AND place_of_performance_congr IS NULL;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop congr info for 5-digit zip derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop county info derivation', submission_id)
    # Deriving county code info for remaining blanks (with zip code)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_perform_county_co = county_number
        FROM zips_grouped
        WHERE place_of_performance_zip5 = zip5
            AND place_of_perform_county_co IS NULL;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop county info derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop city info for transactions with zips derivation', submission_id)
    # Deriving city info for transactions with zips
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_performance_city = city_name
        FROM zip_city
        WHERE place_of_performance_zip5 IS NOT NULL
            AND zip_city.zip_code = place_of_performance_zip5;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop city info for transactions with zips derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop county info for county ppop derivation', submission_id)
    # Deriving county code info for transactions with ppop code XX**###
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_perform_county_co = RIGHT(place_of_performance_code, 3)
        WHERE place_of_performance_zip5 IS NULL
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop county info for county ppop derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop city info for city ppop derivation', submission_id)
    # Deriving county/city info for transactions with ppop code XX#####
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_perform_county_co = county_number,
            place_of_perform_county_na = county_name,
            place_of_performance_city = feature_name
        FROM city_code AS cc
        WHERE place_of_performance_zip5 IS NULL
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d\d$'
            AND cc.city_code = RIGHT(place_of_performance_code, 5)
            AND cc.state_code = place_of_perfor_state_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop city info for city ppop derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning remaining county name derivation', submission_id)
    # Deriving remaining county names
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET place_of_perform_county_na = county_name
        FROM county_code AS cc
        WHERE place_of_perform_county_na IS NULL
            AND place_of_perform_county_co IS NOT NULL
            AND cc.county_number = place_of_perform_county_co
            AND cc.state_code = place_of_perfor_state_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed remaining county name derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed place of performance location derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_ppop_scope(sess, submission_id):
    """ Deriving place of performance scope values from zip4 and place of performance code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning place of performance scope derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning ppop scope with non-null zip derivation', submission_id)
    # When zip is not null
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_performance_scope = CASE WHEN UPPER(place_of_performance_zip4a) = 'CITY-WIDE'
                                              THEN 'City-wide'
                                              WHEN place_of_performance_zip4a ~ '^\d\d\d\d\d(\-?\d\d\d\d)?$'
                                              THEN 'Single ZIP Code'
                                              ELSE NULL
                                         END
        WHERE COALESCE(place_of_performance_zip4a, '') <> ''
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d[\dR]$';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop scope with non-null zip derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning ppop scope with null zip derivation', submission_id)
    # When zip is null
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_performance_scope = CASE WHEN UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d[\dR]$'
                                              THEN 'City-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$'
                                              THEN 'County-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\*\*\*$'
                                              THEN 'State-wide'
                                              WHEN UPPER(place_of_performance_code) ~ '^00\*\*\*\*\*$'
                                              THEN 'Multi-state'
                                              WHEN UPPER(place_of_performance_code) ~ '^00FORGN$'
                                              THEN 'Foreign'
                                              ELSE NULL
                                         END
        WHERE COALESCE(place_of_performance_zip4a, '') = '';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed ppop scope with null zip derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed place of performance scope derivation', submission_id, start_time)


def derive_le_location_data(sess, submission_id):
    """ Deriving place of performance location values

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning legal entity location derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning legal entity location with 9 digit zip derivation derivation', submission_id)
    # Deriving congressional, county, and state info for records with a 9 digit zip
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_congressional = CASE WHEN legal_entity_congressional IS NULL
                                              THEN congressional_district_no
                                              ELSE legal_entity_congressional
                                         END,
            legal_entity_county_code = county_number,
            legal_entity_state_code = state_abbreviation
        FROM zips
        WHERE legal_entity_zip_last4 IS NOT NULL
            AND legal_entity_zip_last4 = zip_last4
            AND legal_entity_zip5 = zip5;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity location with 9 digit zip derivation derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity congressional for 5-digit zip derivation', submission_id)
    # Deriving congressional info for remaining blanks (with zip code)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_congressional = congressional_district_no
        FROM zips_grouped
        WHERE legal_entity_zip5 = zip5
            AND legal_entity_congressional IS NULL;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity congressional for 5-digit zip derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity county and state derivation', submission_id)
    # Deriving county and state code info for remaining blanks (with zip code)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_county_code = county_number,
            legal_entity_state_code = state_abbreviation
        FROM zips_grouped
        WHERE legal_entity_zip5 = zip5
            AND legal_entity_county_code IS NULL;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity county and state derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity county names with zips derivation', submission_id)
    # Deriving county names for records with zips (type 2 and 3)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_county_name = county_name
        FROM county_code AS cc
        WHERE legal_entity_zip5 IS NOT NULL
            AND cc.county_number = legal_entity_county_code
            AND cc.state_code = legal_entity_state_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity county names with zips derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity state names with zips derivation', submission_id)
    # Deriving state names for records with zips (type 2 and 3)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_state_name = state_name
        FROM states
        WHERE legal_entity_zip5 IS NOT NULL
            AND states.state_code = legal_entity_state_code;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity state names with zips derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity city info with zips derivation', submission_id)
    # Deriving city info for records with zips (type 2 and 3)
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_city_name = city_name
        FROM zip_city
        WHERE legal_entity_zip5 IS NOT NULL
            AND zip_city.zip_code = legal_entity_zip5;
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity city info with zips derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity location info record type 1 county format derivation', submission_id)
    # Deriving county, state, and congressional info for county format ppop codes in record type 1
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_county_code = place_of_perform_county_co,
            legal_entity_county_name = place_of_perform_county_na,
            legal_entity_state_code = place_of_perfor_state_code,
            legal_entity_state_name = place_of_perform_state_nam,
            legal_entity_congressional = place_of_performance_congr
        WHERE record_type = 1
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity location info record type 1 county format derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning legal entity location info record type 1 state format derivation', submission_id)
    # Deriving county, state, and congressional info for state format ppop codes in record type 1
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET legal_entity_state_code = place_of_perfor_state_code,
            legal_entity_state_name = place_of_perform_state_nam,
            legal_entity_congressional = place_of_performance_congr
        WHERE record_type = 1
            AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\*\*\*$';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed legal entity location info record type 1 state format derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed legal entity location derivation', submission_id, start_time)


def derive_office_data(sess, submission_id):
    """ Deriving office data

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning office data derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning office data record type not 1 derivation', submission_id)
    # Deriving office codes for record type not 1
    query = """
        WITH awards AS
            (SELECT DISTINCT UPPER(fain) AS upper_fain,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM tmp_fabs_{submission_id}
            WHERE record_type <> '1'),
        min_date AS
            (SELECT CAST(MIN(action_date) AS DATE) AS min_date,
                UPPER(fain) AS upper_fain,
                UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type <> '1'
                AND EXISTS (
                    SELECT 1
                    FROM awards
                    WHERE upper_fain = UPPER(fain)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                )
            GROUP BY UPPER(fain), UPPER(awarding_sub_tier_agency_c)),
        office_info AS
            (SELECT awarding_office_code AS awarding_office_code,
                funding_office_code AS funding_office_code,
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
                        AND cast_as_date(pafa.action_date) = min_date
                )),
        filtered_offices AS
            (SELECT award_modification_amendme,
                upper_fain,
                upper_sub_tier,
                aw_office.office_code AS awarding_office_code,
                fund_office.office_code AS funding_office_code
            FROM office_info AS oi
            LEFT JOIN office AS aw_office
                ON aw_office.office_code = UPPER(oi.awarding_office_code)
                AND aw_office.financial_assistance_awards_office IS TRUE
            LEFT JOIN office AS fund_office
                ON fund_office.office_code = UPPER(oi.funding_office_code)
                AND (fund_office.contract_funding_office IS TRUE
                    OR fund_office.financial_assistance_funding_office IS TRUE))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET awarding_office_code = CASE WHEN pafa.awarding_office_code IS NULL
                                        THEN fo.awarding_office_code
                                        ELSE pafa.awarding_office_code
                                   END,
            funding_office_code = CASE WHEN pafa.funding_office_code IS NULL
                                       THEN fo.funding_office_code
                                       ELSE pafa.funding_office_code
                                  END
        FROM filtered_offices AS fo
        WHERE COALESCE(pafa.award_modification_amendme, '') <> COALESCE(fo.award_modification_amendme, '')
            AND upper_fain = UPPER(fain)
            AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
            AND record_type <> '1';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Beginning office data record type not 1 derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning office data record type 1 derivation', submission_id)
    # Deriving office codes for record type 1
    query = """
        WITH awards AS
            (SELECT DISTINCT UPPER(uri) AS upper_uri,
            UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM tmp_fabs_{submission_id}
            WHERE record_type = '1'),
        min_date AS
            (SELECT CAST(MIN(action_date) AS DATE) AS min_date,
            UPPER(uri) AS upper_uri, UPPER(awarding_sub_tier_agency_c) AS upper_sub_tier
            FROM published_award_financial_assistance AS pafa
            WHERE is_active IS TRUE
                AND record_type = '1'
                AND EXISTS (
                    SELECT 1
                    FROM awards
                    WHERE upper_uri = UPPER(uri)
                        AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
                )
            GROUP BY UPPER(uri), UPPER(awarding_sub_tier_agency_c)),
        office_info AS
            (SELECT awarding_office_code AS awarding_office_code,
                funding_office_code AS funding_office_code,
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
                        AND cast_as_date(pafa.action_date) = min_date
                )),
        filtered_offices AS
            (SELECT award_modification_amendme,
                upper_uri,
                upper_sub_tier,
                aw_office.office_code AS awarding_office_code,
                fund_office.office_code AS funding_office_code
            FROM office_info AS oi
            LEFT JOIN office AS aw_office
                ON aw_office.office_code = UPPER(oi.awarding_office_code)
                AND aw_office.financial_assistance_awards_office IS TRUE
            LEFT JOIN office AS fund_office
                ON fund_office.office_code = UPPER(oi.funding_office_code)
                AND (fund_office.contract_funding_office IS TRUE
                    OR fund_office.financial_assistance_funding_office IS TRUE))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET awarding_office_code = CASE WHEN pafa.awarding_office_code IS NULL
                                        THEN fo.awarding_office_code
                                        ELSE pafa.awarding_office_code
                                   END,
            funding_office_code = CASE WHEN pafa.funding_office_code IS NULL
                                       THEN fo.funding_office_code
                                       ELSE pafa.funding_office_code
                                  END
        FROM filtered_offices AS fo
        WHERE COALESCE(pafa.award_modification_amendme, '') <> COALESCE(fo.award_modification_amendme, '')
            AND upper_uri = UPPER(uri)
            AND upper_sub_tier = UPPER(awarding_sub_tier_agency_c)
            AND record_type = '1';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed office data record type 1 derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning awarding office name derivation', submission_id)
    # Deriving awarding office name
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET awarding_office_name = office_name
        FROM office
        WHERE office_code = UPPER(awarding_office_code);
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed awarding office name derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning funding office name derivation', submission_id)
    # Deriving funding office name
    query = """
        UPDATE tmp_fabs_{submission_id}
        SET funding_office_name = office_name
        FROM office
        WHERE office_code = UPPER(funding_office_code);
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed funding office name derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed office data derivation', submission_id, start_time)


def derive_le_city_code(sess, submission_id):
    """ Deriving legal entity city code

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning legal entity city code derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET legal_entity_city_code = city_code
        FROM city_code
        WHERE UPPER(TRIM(legal_entity_city_name)) = UPPER(feature_name)
            AND UPPER(TRIM(legal_entity_state_code)) = UPPER(state_code);
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed legal entity city code derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_ppop_country_name(sess, submission_id):
    """ Deriving place of performance country name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning place of performance country name derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_perform_country_n = country_name
        FROM country_code
        WHERE country_code.country_code = UPPER(place_of_perform_country_c);
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed place of performance country name derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_le_country_name(sess, submission_id):
    """ Deriving legal entity country name

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning legal entity country name derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET legal_entity_country_name = country_name
        FROM country_code
        WHERE country_code.country_code = UPPER(legal_entity_country_code);
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed legal entity country name derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_pii_redacted_ppop_data(sess, submission_id):
    """ Deriving ppop code and location data for PII-redacted records

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning PII redacted information derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning PII redacted USA records derivation', submission_id)
    # Deriving information for USA records
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
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
        WHERE record_type = 3
            AND UPPER(legal_entity_country_code) = 'USA';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed PII redacted USA records derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning PII redacted non-USA records derivation', submission_id)
    # Deriving information for non-USA records
    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET place_of_performance_code = '00FORGN',
            place_of_perform_country_c = legal_entity_country_code,
            place_of_perform_country_n = legal_entity_country_name,
            place_of_performance_city = legal_entity_foreign_city,
            place_of_performance_forei = legal_entity_foreign_city
        WHERE record_type = 3
            AND UPPER(legal_entity_country_code) <> 'USA';
    """
    res = sess.execute(query.format(submission_id=submission_id))
    log_derivation('Completed PII redacted non-USA records derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed PII redacted information derivation', submission_id, start_time)


def derive_parent_duns(sess, submission_id):
    """ Deriving parent DUNS name and number from SAM API

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning parent DUNS derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET ultimate_parent_legal_enti = duns.ultimate_parent_legal_enti,
            ultimate_parent_unique_ide = duns.ultimate_parent_unique_ide
        FROM duns
        WHERE pafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND (duns.ultimate_parent_legal_enti IS NOT NULL
                OR duns.ultimate_parent_unique_ide IS NOT NULL);
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed parent DUNS derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_executive_compensation(sess, submission_id):
    """ Deriving Executive Compensation information from DUNS.

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning executive compensation derivation', submission_id)

    query = """
        UPDATE tmp_fabs_{submission_id} AS pafa
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
        WHERE pafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND duns.high_comp_officer1_full_na IS NOT NULL;
    """
    res = sess.execute(query.format(submission_id=submission_id))

    log_derivation('Completed executive compensation derivation, '
                   'updated {}'.format(res.rowcount), submission_id, start_time)


def derive_labels(sess, submission_id):
    """ Deriving labels for codes entered by the user

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    start_time = datetime.now()
    log_derivation('Beginning label derivation', submission_id)

    query_start = datetime.now()
    log_derivation('Beginning action type label derivation', submission_id)
    # Action type description derivation
    action_type_values = '), ('.join('\'{}\', \'{}\''.format(name, desc) for name, desc in ACTION_TYPE_DICT.items())
    query = """
        WITH action_type_desc AS
            (SELECT *
            FROM (VALUES ({action_types})) as action_type_desc(letter, description))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET action_type_description = description
        FROM action_type_desc AS atd
        WHERE atd.letter = UPPER(pafa.action_type);
    """
    res = sess.execute(query.format(submission_id=submission_id, action_types=action_type_values))
    log_derivation('Completed action type label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning assistance type label derivation', submission_id)
    # Assistance type description derivation
    assistance_type_values = '), ('.join('\'{}\', \'{}\''.format(name, desc)
                                         for name, desc in ASSISTANCE_TYPE_DICT.items())
    query = """
        WITH assistance_type_description AS
            (SELECT *
            FROM (VALUES ({assistance_types})) as assistance_type_description(letter, description))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET assistance_type_desc = description
        FROM assistance_type_description AS atd
        WHERE atd.letter = UPPER(pafa.assistance_type);
    """
    res = sess.execute(query.format(submission_id=submission_id, assistance_types=assistance_type_values))
    log_derivation('Completed assistance type label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning cdi label derivation', submission_id)
    # CorrectionDeleteIndicator description derivation
    cdi_values = '), ('.join('\'{}\', \'{}\''.format(name, desc) for name, desc in CORRECTION_DELETE_IND_DICT.items())
    query = """
        WITH cdi_desc AS
            (SELECT *
            FROM (VALUES ({cdi_types})) as cdi_desc(letter, description))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET correction_delete_ind_desc = description
        FROM cdi_desc
        WHERE cdi_desc.letter = UPPER(pafa.correction_delete_indicatr);
    """
    res = sess.execute(query.format(submission_id=submission_id, cdi_types=cdi_values))
    log_derivation('Completed cdi label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning record type label derivation', submission_id)
    # Record Type description derivation
    record_type_values = '), ('.join('{}, \'{}\''.format(name, desc) for name, desc in RECORD_TYPE_DICT.items())
    query = """
        WITH record_type_desc AS
            (SELECT *
            FROM (VALUES ({record_types})) as record_type_desc(letter, description))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET record_type_description = description
        FROM record_type_desc AS rtd
        WHERE rtd.letter = pafa.record_type;
    """
    res = sess.execute(query.format(submission_id=submission_id, record_types=record_type_values))
    log_derivation('Completed record type label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning business funds ind label derivation', submission_id)
    # Business Funds Indicator description derivation
    business_funds_values = '), ('.join('\'{}\', \'{}\''.format(name, desc)
                                        for name, desc in BUSINESS_FUNDS_IND_DICT.items())
    query = """
        WITH business_funds_ind_description AS
            (SELECT *
            FROM (VALUES ({business_funds_ind})) as business_funds_ind_description(letter, description))
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET business_funds_ind_desc = description
        FROM business_funds_ind_description AS bfid
        WHERE bfid.letter = UPPER(pafa.business_funds_indicator);
    """
    res = sess.execute(query.format(submission_id=submission_id, business_funds_ind=business_funds_values))
    log_derivation('Completed business funds ind label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    query_start = datetime.now()
    log_derivation('Beginning business type label derivation', submission_id)
    # Business types description derivation
    business_types_values = '), ('.join('\'{}\', \'{}\''.format(name, desc)
                                        for name, desc in BUSINESS_TYPE_DICT.items())
    query = """
        WITH business_type_desc AS
            (SELECT *
            FROM(VALUES ({business_types})) as business_type_desc(letter, description)),
        aggregated_business_types AS
            (SELECT published_award_financial_assistance_id,
                string_agg(btd.description, ';' order by ordinality) AS aggregated
            FROM tmp_fabs_{submission_id} AS pafa,
                unnest(string_to_array(pafa.business_types, NULL)) WITH ORDINALITY AS u(business_type_id, ordinality)
            LEFT JOIN business_type_desc AS btd
                ON btd.letter = UPPER(business_type_id)
            GROUP BY published_award_financial_assistance_id)
        UPDATE tmp_fabs_{submission_id} AS pafa
        SET business_types_desc = abt.aggregated
        FROM aggregated_business_types AS abt
        WHERE
            abt.published_award_financial_assistance_id = pafa.published_award_financial_assistance_id;
    """
    res = sess.execute(query.format(submission_id=submission_id, business_types=business_types_values))
    log_derivation('Completed business type label derivation, '
                   'updated {}'.format(res.rowcount), submission_id, query_start)

    log_derivation('Completed label derivation', submission_id, start_time)


def fabs_derivations(sess, submission_id):
    """ Performs derivations related to publishing a FABS submission

        Args:
            sess: the current DB session
            submission_id: The ID of the submission derivations are being run for
    """
    # TODO: Decide if we want to include the job in the logs
    # TODO: Decide if we want to log each SQL query (including start/end or just the end) or just each function
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
