import factory
from factory import fuzzy
from dataactcore.models import userModel

class UserFactory(factory.Factory):
    class Meta:
        model = userModel.User

    username = fuzzy.FuzzyText()
    email = fuzzy.FuzzyText()
    name = fuzzy.FuzzyText()
    cgac_code = fuzzy.FuzzyText()
    title = fuzzy.FuzzyText()