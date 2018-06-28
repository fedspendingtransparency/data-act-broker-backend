import factory
from factory import fuzzy
from dataactcore.models import userModel
from dataactcore.models.lookups import PERMISSION_TYPE_DICT


class UserFactory(factory.Factory):
    class Meta:
        model = userModel.User

    user_id = fuzzy.FuzzyInteger(9999)
    username = fuzzy.FuzzyText()
    email = fuzzy.FuzzyText()
    name = fuzzy.FuzzyText()
    title = fuzzy.FuzzyText()

    @classmethod
    def with_cgacs(cls, *cgacs, **kwargs):
        perm = PERMISSION_TYPE_DICT['reader']
        kwargs['affiliations'] = [userModel.UserAffiliation(cgac=cgac, permission_type_id=perm) for cgac in cgacs]
        return cls(**kwargs)
