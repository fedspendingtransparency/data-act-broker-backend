import logging
from operator import attrgetter
import re
import requests
import time
import xmltodict

from dateutil.parser import parse
from flask import g

from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.interfaces.db import GlobalDB
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy import func

from dataactcore.models.userModel import User, UserAffiliation
from dataactcore.models.domainModels import CGAC
from dataactcore.models.jobModels import Submission
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.function_bag import get_email_template, check_correct_password, updateLastLogin
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import (
    PERMISSION_SHORT_DICT, USER_STATUS_DICT)


logger = logging.getLogger(__name__)


class AccountHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    FRONT_END = ""
    INACTIVITY_THRESHOLD = 120 # Days a user's account can be unused before being marked as inactive
    ALLOWED_PASSWORD_ATTEMPTS = 3 # Number of allowed login attempts before account is locked
    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request, bcrypt=None, isLocal=False):
        """ Creates the Login Handler

        Args:
            request - Flask request object
            bcrypt - Bcrypt object associated with app
        """
        self.isLocal = isLocal
        self.request = request
        self.bcrypt = bcrypt

    def checkPassword(self,password):
        """Checks to make sure the password is valid"""
        if( re.search(r"[\[\]\{\}~!@#$%^,.?;<>]", password) is None or len(password) < 8 or re.search(r"\d", password) is None or
            re.search(r"[A-Z]", password) is None or re.search(r"[a-z]", password) is None) :
            return False
        return True

    def login(self,session):
        """

        Logs a user in if their password matches

        arguments:

        session  -- (Session) object from flask

        return the response object

        """
        try:
            sess = GlobalDB.db().session
            safe_dictionary = RequestDictionary(self.request)

            username = safe_dictionary.getValue('username')

            password = safe_dictionary.getValue('password')

            try:
                user = sess.query(User).filter(func.lower(User.email) == func.lower(username)).one()
            except Exception:
                raise ValueError("Invalid username and/or password")

            if user.user_status_id != USER_STATUS_DICT["approved"]:
                raise ValueError("Invalid username and/or password")

            # Only check if user is active after they've logged in for the first time
            if user.last_login_date is not None and self.isAccountExpired(user):
                raise ValueError("Your account has expired. Please contact an administrator.")

            # for whatever reason, your account is not active, therefore it's locked
            if not user.is_active:
                raise ValueError("Your account has been locked. Please contact an administrator.")

            try:
                if check_correct_password(user,password,self.bcrypt):
                    # We have a valid login

                    # Reset incorrect password attempt count to 0
                    self.reset_password_count(user)

                    return self.create_session_and_response(session, user)
                else :
                    # increase incorrect password attempt count by 1
                    # if this is the 3rd incorrect attempt, lock account
                    self.incrementPasswordCount(user)
                    if user.incorrect_password_attempts == 3:
                        raise ValueError("Your account has been locked due to too many failed login attempts. Please contact an administrator.")

                    raise ValueError("Invalid username and/or password")
            except ValueError as ve:
                LoginSession.logout(session)
                raise ve
            except Exception as e:
                LoginSession.logout(session)
                raise e

        except (TypeError, KeyError, NotImplementedError) as e:
            # Return a 400 with appropriate message
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ValueError as e:
            # Return a 401 for login denied
            return JsonResponse.error(e,StatusCode.LOGIN_REQUIRED)
        except Exception as e:
            # Return 500
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)


    def max_login(self,session):
        """

        Logs a user in if their password matches

        arguments:

        session  -- (Session) object from flask

        return the reponse object

        """
        try:
            safeDictionary = RequestDictionary(self.request)

            # Obtain POST content
            ticket = safeDictionary.getValue("ticket")
            service = safeDictionary.getValue('service')

            # Call MAX's serviceValidate endpoint and retrieve the response
            max_dict = self.get_max_dict(ticket, service)

            if not 'cas:authenticationSuccess' in max_dict['cas:serviceResponse']:
                raise ValueError("You have failed to login successfully with MAX")
            cas_attrs = max_dict['cas:serviceResponse']['cas:authenticationSuccess']['cas:attributes']

            # Grab the email and list of groups from MAX's response
            email = cas_attrs['maxAttribute:Email-Address']

            try:
                sess = GlobalDB.db().session
                user = sess.query(User).filter(func.lower(User.email) == func.lower(email)).one_or_none()

                # If the user does not exist, create them since they are allowed to access the site because they got
                # past the above group membership checks
                if user is None:
                    user = User()

                    first_name = cas_attrs['maxAttribute:First-Name']
                    middle_name = cas_attrs['maxAttribute:Middle-Name']
                    last_name = cas_attrs['maxAttribute:Last-Name']

                    user.email = email

                    # Check for None first so the condition can short-circuit without
                    # having to worry about calling strip() on a None object
                    if middle_name is None or middle_name.strip() == '':
                        user.name = first_name + " " + last_name
                    else:
                        user.name = first_name + " " + middle_name[0] + ". " + last_name
                    user.user_status_id = user.user_status_id = USER_STATUS_DICT['approved']


                set_max_perms(user, cas_attrs['maxAttribute:GroupList'])

                sess.add(user)
                sess.commit()

            except MultipleResultsFound:
                raise ValueError("An error occurred during login.")

            return self.create_session_and_response(session, user)

        except (TypeError, KeyError, NotImplementedError) as e:
            # Return a 400 with appropriate message
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ValueError as e:
            # Return a 401 for login denied
            return JsonResponse.error(e,StatusCode.LOGIN_REQUIRED)
        except Exception as e:
            # Return 500
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
        return self.response

    def get_max_dict(self, ticket, service):
        url = CONFIG_BROKER['cas_service_url'].format(ticket, service)
        max_xml = requests.get(url).content
        return xmltodict.parse(max_xml)

    @staticmethod
    def create_session_and_response(session, user):
        """Create a session."""
        LoginSession.login(session, user.user_id)
        updateLastLogin(user)
        data = json_for_user(user)
        data['message'] = 'Login successful'
        return JsonResponse.create(StatusCode.OK, data)

    def logout(self,session):
        """

        This function removes the session from the session table if currently logged in, and then returns a success message

        arguments:

        session  -- (Session) object from flask

        return the reponse object

        """
        # Call session handler
        LoginSession.logout(session)
        return JsonResponse.create(StatusCode.OK,{"message":"Logout successful"})

    def list_user_emails(self):
        """ List user names and emails """
        sess = GlobalDB.db().session
        users = sess.query(User).\
            filter_by(user_status_id=USER_STATUS_DICT['approved']).\
            filter_by(is_active=True)
        if not g.user.website_admin:
            relevant_cgacs = [aff.cgac_id for aff in g.user.affiliations]
            subquery = sess.query(UserAffiliation.user_id).\
                filter(UserAffiliation.cgac_id.in_(relevant_cgacs))
            users = users.filter(User.user_id.in_(subquery))

        user_info = [
            {"id": user.user_id, "name": user.name, "email": user.email}
            for user in users
        ]
        return JsonResponse.create(StatusCode.OK, {"users": user_info})

    def send_reset_password_email(self, user, system_email, email=None, unlock_user=False):
        sess = GlobalDB.db().session
        if email is None:
            email = user.email

        # User must be approved and active to reset password
        if user.user_status_id != USER_STATUS_DICT["approved"]:
            raise ResponseException("User must be approved before resetting password", StatusCode.CLIENT_ERROR)
        elif not unlock_user and not user.is_active:
            raise ResponseException("User is locked, cannot reset password", StatusCode.CLIENT_ERROR)

        # If unlocking a user, wipe out current password
        if unlock_user:
            user.salt = None
            user.password_hash = None

        sess.commit()
        # Send email with token
        email_token = sesEmail.createToken(email, "password_reset")
        link = "".join([AccountHandler.FRONT_END, '#/forgotpassword/', email_token])
        email_template = {'[URL]': link}
        template_type = "unlock_account" if unlock_user else "reset_password"
        new_email = sesEmail(user.email, system_email, templateType=template_type, parameters=email_template)
        new_email.send()

    def isAccountExpired(self, user):
        """ Checks user's last login date against inactivity threshold, marks account as inactive if expired

        Args:
            user: User object to check

        """
        today = parse(time.strftime("%c"))
        daysActive = (today-user.last_login_date).days
        if daysActive >= self.INACTIVITY_THRESHOLD:
            self.lockAccount(user)
            return True
        return False

    def reset_password_count(self, user):
        """ Resets the number of failed attempts when a user successfully logs in

        Args:
            user: User object to be changed
        """
        if user.incorrect_password_attempts != 0:
            sess = GlobalDB.db().session
            user.incorrect_password_attempts = 0
            sess.commit()

    def incrementPasswordCount(self, user):
        """ Records a failed attempt to log in.  If number of failed attempts is higher than threshold, locks account.

        Args:
            user: User object to be changed

        Returns:

        """
        if user.incorrect_password_attempts < self.ALLOWED_PASSWORD_ATTEMPTS:
            sess = GlobalDB.db().session
            user.incorrect_password_attempts += 1
            if user.incorrect_password_attempts == self.ALLOWED_PASSWORD_ATTEMPTS:
                self.lockAccount(user)
            sess.commit()

    def lockAccount(self, user):
        """ Lock this user's account by marking it as inactive

        Args:
            user: User object to be locked
        """
        sess = GlobalDB.db().session
        user.is_active = False
        sess.commit()

    def set_skip_guide(self, session):
        """ Set current user's skip guide parameter """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)
        try:
            if 'skip_guide' not in request_dict:
                raise ResponseException(
                    "Must include skip_guide parameter",
                    StatusCode.CLIENT_ERROR
                )
            skip_guide = str(request_dict['skip_guide']).lower()
            if skip_guide not in ("true", "false"):
                raise ResponseException(
                    "skip_guide must be true or false",
                    StatusCode.CLIENT_ERROR
                )
            g.user.skip_guide = skip_guide == "true"
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)
        sess.commit()
        return JsonResponse.create(
            StatusCode.OK,
            {"message": "skip_guide set successfully",
             "skip_guide":skip_guide}
        )

    def email_users(self, system_email, session):
        """ Send email notification to list of users """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)
        required = ('users', 'submission_id', 'email_template')
        try:
            if any(field not in request_dict for field in required):
                raise ResponseException(
                    "Email users route requires users, email_template, and "
                    "submission_id", StatusCode.CLIENT_ERROR
                )
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        user_ids = request_dict['users']
        submission_id = request_dict['submission_id']
        # Check if submission id is valid
        sess.query(Submission).filter_by(submission_id=submission_id).one()

        template_type = request_dict['email_template']
        # Check if email template type is valid
        get_email_template(template_type)

        users = []

        link = "".join([AccountHandler.FRONT_END, '#/reviewData/', str(submission_id)])
        email_template = {'[REV_USER_NAME]': g.user.name, '[REV_URL]': link}

        for user_id in user_ids:
            # Check if user id is valid, if so add User object to array
            users.append(sess.query(User).filter(User.user_id == user_id).one())

        for user in users:
            new_email = sesEmail(user.email, system_email, templateType=template_type, parameters=email_template)
            new_email.send()

        return JsonResponse.create(StatusCode.OK, {"message": "Emails successfully sent"})


def perms_to_affiliations(perms):
    """Convert a list of perms from MAX to a list of UserAffiliations. Filter
    out and log any malformed perms"""
    available_codes = {
        cgac.cgac_code:cgac
        for cgac in GlobalDB.db().session.query(CGAC)
    }
    for perm in perms:
        components = perm.split('-PERM_')
        if len(components) != 2:
            logger.warning('Malformed permission: %s', perm)
            continue

        cgac_code, perm_level = components
        perm_level = perm_level.lower()
        if cgac_code not in available_codes or perm_level not in 'rws':
            logger.warning('Malformed permission: %s', perm)
            continue

        yield UserAffiliation(
            cgac=available_codes[cgac_code],
            permission_type_id=PERMISSION_SHORT_DICT[perm_level]
        )


def best_affiliation(affiliations):
    """If a user has multiple permissions for a single agency, select the
    best"""
    by_agency = {}
    affiliations = sorted(affiliations, key=attrgetter('permission_type_id'))
    for affiliation in affiliations:
        by_agency[affiliation.cgac] = affiliation
    return by_agency.values()


def set_max_perms(user, max_group_list):
    """Convert the user group lists present on MAX into a list of
    UserAffiliations and/or website_admin status.

    Permissions are encoded as a comma-separated list of
    {parent-group}-CGAC_{cgac-code}-PERM_{one-of-R-W-S}
    or
    {parent-group}-CGAC_SYS to indicate website_admin"""
    prefix = CONFIG_BROKER['parent_group'] + '-CGAC_'

    # Each group name that we care about begins with the prefix, but once we
    # have that list, we don't need the prefix anymore, so trim it off.
    perms = [group_name[len(prefix):]
             for group_name in max_group_list.split(',')
             if group_name.startswith(prefix)]
    if 'SYS' in perms:
        user.affiliations = []
        user.cgac_code = 'SYS'
        user.website_admin = True
    else:
        affiliations = list(best_affiliation(perms_to_affiliations(perms)))

        if affiliations:
            user.cgac_code = affiliations[0].cgac.cgac_code
        else:
            user.cgac_code = None

        user.affiliations = affiliations
        user.website_admin = False


def json_for_user(user):
    """Convert the provided user to a dictionary (for JSON)"""
    sess = GlobalDB.db().session
    agency_name = sess.query(CGAC.agency_name).\
        filter(CGAC.cgac_code == user.cgac_code).\
        one_or_none()
    return {
        "user_id": user.user_id,
        "name": user.name,
        "agency_name": agency_name,
        "cgac_code": user.cgac_code,
        "title": user.title,
        "skip_guide": user.skip_guide,
        "website_admin": user.website_admin,
        "affiliations": [{"agency_name": affil.cgac.agency_name,
                          "permission": affil.permission_type_name}
                         for affil in user.affiliations]
    }
