import re
import time
import requests
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
from dataactcore.models.lookups import USER_STATUS_DICT, PERMISSION_TYPE_DICT, PERMISSION_MAP


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
            parent_group = CONFIG_BROKER['parent_group']

            # Call MAX's serviceValidate endpoint and retrieve the response
            max_dict = self.get_max_dict(ticket, service)

            if not 'cas:authenticationSuccess' in max_dict['cas:serviceResponse']:
                raise ValueError("You have failed to login successfully with MAX")

            # Grab the email and list of groups from MAX's response
            email = max_dict['cas:serviceResponse']['cas:authenticationSuccess']['cas:attributes']['maxAttribute:Email-Address']
            group_list_all = max_dict['cas:serviceResponse']['cas:authenticationSuccess']['cas:attributes']['maxAttribute:GroupList'].split(',')
            group_list = [g for g in group_list_all if g.startswith(parent_group)]

            cgac_group = [g for g in group_list if g.startswith(parent_group+"-CGAC_")]

            try:
                sess = GlobalDB.db().session
                user = sess.query(User).filter(func.lower(User.email) == func.lower(email)).one_or_none()

                # If the user does not exist, create them since they are allowed to access the site because they got
                # past the above group membership checks
                if user is None:
                    user = User()

                    first_name = max_dict["cas:serviceResponse"]['cas:authenticationSuccess']['cas:attributes'][
                        'maxAttribute:First-Name']
                    middle_name = max_dict["cas:serviceResponse"]['cas:authenticationSuccess']['cas:attributes'][
                        'maxAttribute:Middle-Name']
                    last_name = max_dict["cas:serviceResponse"]['cas:authenticationSuccess']['cas:attributes'][
                        'maxAttribute:Last-Name']

                    user.email = email

                    # Check for None first so the condition can short-circuit without
                    # having to worry about calling strip() on a None object
                    if middle_name is None or middle_name.strip() == '':
                        user.name = first_name + " " + last_name
                    else:
                        user.name = first_name + " " + middle_name[0] + ". " + last_name
                    user.user_status_id = user.user_status_id = USER_STATUS_DICT['approved']


                # update user's cgac based on their current membership
                # If part of the SYS agency, use that as the cgac otherwise
                # use the first agency provided
                if [g for g in cgac_group if g.endswith("SYS")]:
                    grant_superuser(user)
                else:
                    grant_highest_permission(user, group_list, cgac_group)

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

    def create_session_and_response(self, session, user):
        """Create a session."""
        LoginSession.login(session, user.user_id)

        sess = GlobalDB.db().session
        updateLastLogin(user)
        agency_name = sess.query(CGAC.agency_name).\
            filter(CGAC.cgac_code == user.cgac_code).\
            one_or_none()
        return JsonResponse.create(StatusCode.OK, {"message": "Login successful", "user_id": int(user.user_id),
                                                   "name": user.name, "title": user.title,
                                                   "agency_name": agency_name,
                                                   "cgac_code": user.cgac_code, "permission": user.permission_type_id})

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
        try:
            users = sess.query(User).filter_by(
                cgac_code=g.user.cgac_code,
                user_status_id=USER_STATUS_DICT["approved"],
                is_active=True
            ).all()
        except ValueError as exc:
            # Client provided a bad status
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)
        user_info = []
        for user in users:
            this_info = {"id":user.user_id, "name": user.name, "email": user.email}
            user_info.append(this_info)
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

    def get_current_user(self,session):
        """

        Gets the current user information

        arguments:

        session  -- (Session) object from flask

        return the response object with the current user information

        """
        sess = GlobalDB.db().session
        agency_name = sess.query(CGAC.agency_name).\
            filter(CGAC.cgac_code == g.user.cgac_code).\
            one_or_none()
        return JsonResponse.create(StatusCode.OK, {
            "user_id": g.user.user_id,
            "name": g.user.name,
            "agency_name": agency_name,
            "cgac_code": g.user.cgac_code,
            "title": g.user.title,
            "permission": g.user.permission_type_id,
            "skip_guide": g.user.skip_guide,
            "website_admin": g.user.website_admin
        })

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


def grant_superuser(user):
    user.cgac_code = 'SYS'
    user.website_admin = True
    user.permission_type_id = PERMISSION_TYPE_DICT['writer']


def grant_highest_permission(user, group_list, cgac_group_list):
    """Find the highest permission within the provided cgac_group; set that as
    the user's permission_type_id"""
    sess = GlobalDB.db().session
    user.website_admin = False
    if not cgac_group_list:
        user.cgac_code = None
        user.permission_type_id = None
        return

    cgac_group = cgac_group_list[0]
    permission_group = [g for g in group_list
                        if g.startswith(cgac_group + "-PERM_")]
    # Check if a user has been placed in a specific group. If not, deny access
    if not permission_group:
        user.permission_type_id = None
    else:
        user.cgac_code = cgac_group[-3:]
        perms = [perm[-1].lower() for perm in permission_group]
        ordered_perms = sorted(
            PERMISSION_MAP.items(), key=lambda pair: pair[1]['order'])
        for key, permission in ordered_perms:
            name = permission['name']
            if key in perms:
                user.permission_type_id = PERMISSION_TYPE_DICT[name]
                break
    user.affiliations = [UserAffiliation(
        cgac=sess.query(CGAC).filter_by(cgac_code=user.cgac_code).one(),
        permission_type_id=user.permission_type_id
    )]
