import factory
from factory import fuzzy

from dataactcore.models import domainModels


class SF133Factory(factory.Factory):
    class Meta:
        model = domainModels.SF133

    sf133_id = None
    agency_identifier = fuzzy.FuzzyText()
    allocation_transfer_agency = fuzzy.FuzzyText()
    availability_type_code = fuzzy.FuzzyText()
    beginning_period_of_availa = fuzzy.FuzzyText()
    ending_period_of_availabil = fuzzy.FuzzyText()
    main_account_code = fuzzy.FuzzyText()
    sub_account_code = fuzzy.FuzzyText()
    tas = fuzzy.FuzzyText()
    fiscal_year = fuzzy.FuzzyInteger(2010, 2040)
    period = fuzzy.FuzzyInteger(1, 4)
    line = fuzzy.FuzzyInteger(1, 9999)
    amount = 0
