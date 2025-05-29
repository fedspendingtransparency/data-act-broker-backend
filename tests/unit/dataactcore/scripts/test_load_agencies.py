import pytest
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.setup import load_agencies
from dataactcore.models.domainModels import CGAC, FREC, SubTierAgency, ExternalDataType, ExternalDataLoadDate


def add_relevant_data_types(sess):
    data_types = sess.query(ExternalDataType).all()
    if len(data_types) == 0:
        agency = ExternalDataType(external_data_type_id=4, name="agency", description="IAE agency data loaded")
        sess.add(agency)
        sess.commit()


def mock_get_agency_file_cgac(base_path):
    test_file = os.path.join("test_agency_codes.csv")
    with open(test_file, "w") as csv_file:
        csv_file.write(
            "CGAC AGENCY CODE,AGENCY NAME,AGENCY ABBREVIATION,FREC,FREC Entity Description,"
            "FREC ABBREVIATION,SUBTIER CODE,SUBTIER NAME,SUBTIER ABBREVIATION,Admin Org Name,ADMIN_ORG,"
            "TOPTIER_FLAG,IS_FREC,FREC CGAC ASSOCIATION,USER SELECTABLE ON USASPENDING.GOV,MISSION,"
            "ABOUT AGENCY DATA,WEBSITE,CONGRESSIONAL JUSTIFICATION,ICON FILENAME,COMMENT\n"
        )
        csv_file.write("999,,,,,,,,,,,,,,,,,,,,")
    return test_file


def mock_get_agency_file_subtier(base_path):
    test_file = os.path.join("test_agency_codes.csv")
    with open(test_file, "w") as csv_file:
        csv_file.write(
            "CGAC AGENCY CODE,AGENCY NAME,AGENCY ABBREVIATION,FREC,FREC Entity Description,"
            "FREC ABBREVIATION,SUBTIER CODE,SUBTIER NAME,SUBTIER ABBREVIATION,Admin Org Name,ADMIN_ORG,"
            "TOPTIER_FLAG,IS_FREC,FREC CGAC ASSOCIATION,USER SELECTABLE ON USASPENDING.GOV,MISSION,"
            "ABOUT AGENCY DATA,WEBSITE,CONGRESSIONAL JUSTIFICATION,ICON FILENAME,COMMENT\n"
        )
        csv_file.write(",,,,,,TFVA,,,,,,,,,,,,,,")
    return test_file


def test_load_agencies(database, monkeypatch):
    """Test actually loading the defc data"""
    monkeypatch.setattr(load_agencies, "CONFIG_BROKER", {"use_aws": False})

    sess = database.session
    add_relevant_data_types(sess)

    base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    load_agencies.load_agency_data(base_path)

    assert sess.query(ExternalDataLoadDate).filter_by(external_data_type_id=4).count() > 0

    # CGACs
    assert sess.query(CGAC).count() > 0

    usda_cgac = sess.query(CGAC).filter_by(cgac_code="012").one()
    assert usda_cgac.agency_name == "Department of Agriculture (USDA)"
    assert usda_cgac.icon_name == "USDA.jpg"

    fca_cgac = sess.query(CGAC).filter_by(cgac_code="352").one()
    assert fca_cgac.agency_name == "Farm Credit Administration (FCA)"
    assert fca_cgac.icon_name is None

    test_cgac = sess.query(CGAC).filter_by(cgac_code="999").one()
    assert test_cgac.agency_name == "Non-published FABS Vendor Agency (TFVA)"
    assert test_cgac.icon_name is None

    # FRECs
    assert sess.query(FREC).count() > 0

    usda_frec = sess.query(FREC).filter_by(frec_code="1200").one()
    assert usda_frec.agency_name == "Department of Agriculture (USDA)"
    assert usda_frec.icon_name == "USDA.jpg"
    assert usda_frec.cgac_id == usda_cgac.cgac_id

    fca_frec = sess.query(FREC).filter_by(frec_code="7801").one()
    assert fca_frec.agency_name == "Farm Credit Administration (FCA)"
    assert fca_frec.icon_name is None
    assert fca_frec.cgac_id == fca_cgac.cgac_id

    # Subtiers
    assert sess.query(SubTierAgency).count() > 0

    usda_subtier = sess.query(SubTierAgency).filter_by(sub_tier_agency_code="12C2").one()
    assert usda_subtier.sub_tier_agency_name == "Forest Service"
    assert usda_subtier.priority == 2
    assert usda_subtier.is_frec is False
    assert usda_subtier.cgac_id == usda_cgac.cgac_id

    fca_subtier = sess.query(SubTierAgency).filter_by(sub_tier_agency_code="7886").one()
    assert fca_subtier.sub_tier_agency_name == "Farm Credit System Financial Assistance Corporation"
    assert fca_subtier.priority == 2
    assert fca_subtier.is_frec is True
    assert fca_subtier.cgac_id == fca_cgac.cgac_id
    assert fca_subtier.frec_id == fca_frec.frec_id

    test_subtier = sess.query(SubTierAgency).filter_by(sub_tier_agency_code="TFVA").one()
    assert test_subtier.sub_tier_agency_name == "Non-published FABS Vendor Subtier Agency"
    assert test_subtier.priority == 1
    assert test_subtier.is_frec is False
    assert test_subtier.cgac_id == test_cgac.cgac_id

    # Test custom agency/subtier checks
    monkeypatch.setattr("dataactcore.scripts.setup.load_agencies.get_agency_file", mock_get_agency_file_cgac)
    with pytest.raises(ValueError) as val_error:
        load_agencies.load_agency_data(base_path)
    assert (
        str(val_error.value) == "Custom CGAC code found in agency list: 999."
        " Consult the latest agency list with the custom CGAC code."
    )
    os.remove(os.path.join("test_agency_codes.csv"))

    monkeypatch.setattr("dataactcore.scripts.setup.load_agencies.get_agency_file", mock_get_agency_file_subtier)
    with pytest.raises(ValueError) as val_error:
        load_agencies.load_agency_data(base_path)
    assert (
        str(val_error.value) == "Custom Subtier code found in agency list: TFVA."
        " Consult the latest agency list with the custom Subtier code."
    )
    os.remove(os.path.join("test_agency_codes.csv"))
