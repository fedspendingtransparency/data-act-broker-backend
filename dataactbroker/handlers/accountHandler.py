import re
import time
import requests
import xmltodict

from threading import Thread

from dateutil.parser import parse
from flask import session as flaskSession

from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.interfaces.db import GlobalDB
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy import func
from dataactcore.models.userModel import User, EmailToken
from dataactcore.models.domainModels import CGAC
from dataactcore.models.jobModels import Submission
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.function_bag import (get_email_template, check_correct_password, set_user_password, updateLastLogin)
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import USER_STATUS_DICT, PERMISSION_TYPE_DICT, PERMISSION_TYPE_DICT_ID, PERMISSION_MAP

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

            # Deny access if they are not aligned with an agency
            if not cgac_group:
                raise ValueError("You have logged in with MAX but do not have permission to access the broker.")

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
                    grant_highest_permission(user, group_list, cgac_group[0])

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


    def register(self,system_email,session):
        """

        Save user's information into user database.  Associated request body should have keys 'email', 'name', 'cgac_code', and 'title'

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask


        Returns message that registration is successful or error message that fields are not valid

        """
        def ThreadedFunction (username="", title="", cgac_code="", user_email="" , link="") :
            """
            This inner function sends emails in a new thread as there could be lots of admins

            username -- (string) the name of the  user
            title  --   (string) the title of the  user
            cgac_code -- (string) the agency of the  user
            user_email -- (string) the email of the user
            link  -- (string) the broker email link
            """
            agency_name = sess.query(CGAC.agency_name).\
                filter(CGAC.cgac_code == cgac_code).\
                one_or_none()
            agency_name = "Unknown" if agency_name is None else agency_name
            for user in sess.query(User).filter_by(website_admin=True):
                email_template = {'[REG_NAME]': username, '[REG_TITLE]':title, '[REG_AGENCY_NAME]':agency_name,
                                 '[REG_CGAC_CODE]': cgac_code,'[REG_EMAIL]' : user_email,'[URL]':link}
                new_email = sesEmail(user.email, system_email,templateType="account_creation",parameters=email_template)
                new_email.send()

        sess = GlobalDB.db().session
        request_fields = RequestDictionary.derive(self.request)
        try:
            required = ('email', 'name', 'cgac_code', 'title', 'password')
            if any(field not in request_fields for field in required):
                # Missing a required field, return 400
                raise ResponseException(
                    "Request body must include email, name, cgac_code, "
                    "title, and password", StatusCode.CLIENT_ERROR
                )
            if not self.checkPassword(request_fields["password"]):
                raise ResponseException(
                    "Invalid Password", StatusCode.CLIENT_ERROR)
            # Find user that matches specified email
            user = sess.query(User).filter(
                func.lower(User.email) == func.lower(request_fields['email'])
            ).one_or_none()
            if user is None:
                raise ResponseException(
                    "No users with that email", StatusCode.CLIENT_ERROR)
            # Check that user's status is before submission of registration
            bad_statuses = (USER_STATUS_DICT["awaiting_confirmation"], USER_STATUS_DICT["email_confirmed"])
            if user.user_status_id not in bad_statuses:
                # Do not allow duplicate registrations
                raise ResponseException(
                    "User already registered", StatusCode.CLIENT_ERROR)
            # Add user info to database
            user.name = request_fields['name']
            user.cgac_code = request_fields['cgac_code']
            user.title = request_fields['title']
            sess.commit()
            set_user_password(user, request_fields['password'], self.bcrypt)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        user_link= "".join([AccountHandler.FRONT_END, '#/login?redirect=/admin'])
        # Send email to approver list
        email_thread = Thread(target=ThreadedFunction, kwargs=dict(username=user.name,title=user.title,cgac_code=user.cgac_code,user_email=user.email,link=user_link))
        email_thread.start()

        #email user
        email_template = {'[EMAIL]' : system_email}
        new_email = sesEmail(user.email, system_email,templateType="account_creation_user",parameters=email_template)
        new_email.send()

        # Logout and delete token
        LoginSession.logout(session)
        oldToken = sess.query(EmailToken).filter(EmailToken.token == session["token"]).one()
        sess.delete(oldToken)
        # Mark user as awaiting approval
        user.user_status_id = USER_STATUS_DICT["awaiting_approval"]
        sess.commit()
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def create_email_confirmation(self,system_email):
        """

        Creates user record and email

        arguments:

        system_email  -- (string) email used to send messages

        """
        sess = GlobalDB.db().session
        request_fields = RequestDictionary.derive(self.request)
        try:
            if 'email' not in request_fields:
                raise ResponseException(
                    "Request body must include email", StatusCode.CLIENT_ERROR)
            email = request_fields['email']
            if not re.match("[^@]+@[^@]+\.[^@]+",email):
                raise ValueError("Invalid Email Format")
        except (ResponseException, ValueError) as exc:
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)

        try :
            user = sess.query(User).filter(
                func.lower(User.email) == func.lower(request_fields['email'])
            ).one()
        except NoResultFound:
            # Create user with specified email if none is found
            user = User(email=email)
            user.user_status_id = USER_STATUS_DICT["awaiting_confirmation"]
            user.permissions = 0
            sess.add(user)
            sess.commit()
        else:
            try:
                good_statuses = (USER_STATUS_DICT["awaiting_confirmation"],
                                 USER_STATUS_DICT["email_confirmed"])
                if user.user_status_id not in good_statuses:
                    raise ResponseException(
                        "User already registered", StatusCode.CLIENT_ERROR)
            except ResponseException as exc:
                return JsonResponse.error(exc, exc.status)
        email_token = sesEmail.createToken(email, "validate_email")
        link= "".join([AccountHandler.FRONT_END,'#/registration/',email_token])
        email_template = {'[USER]': email, '[URL]':link}
        new_email = sesEmail(email, system_email,templateType="validate_email",parameters=email_template)
        new_email.send()
        return JsonResponse.create(StatusCode.OK,{"message":"Email Sent"})

    def check_email_confirmation_token(self,session):
        """

        Creates user record and email

        arguments:

        session -- (Session) object from flask

        return the response object with a error code and a message

        """
        sess = GlobalDB.db().session
        request_fields = RequestDictionary.derive(self.request)
        try:
            if 'token' not in request_fields:
                raise ResponseException(
                    "Request body must include token",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        token = request_fields['token']
        session["token"] = token
        success, message, errorCode = sesEmail.check_token(token, "validate_email")
        if success:
            #mark session that email can be filled out
            LoginSession.register(session)

            #remove token so it cant be used again
            # The following lines are commented out for issues with registration email links bouncing users back
            # to the original email input page instead of the registration page
            # oldToken = sess.query(EmailToken).filter(EmailToken.token == session["token"]).one()
            # sess.delete(oldToken)
            # sess.commit()

            #set the status only if current status is awaiting confirmation
            user = sess.query(User).filter(func.lower(User.email) == func.lower(message)).one()
            if user.user_status_id == USER_STATUS_DICT["awaiting_confirmation"]:
                user.user_status_id = USER_STATUS_DICT["email_confirmed"]
                sess.commit()
            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})

    def checkPasswordToken(self,session):
        """

        Checks the password token if its valid

        arguments:

        session -- (Session) object from flask

        return the reponse object with a error code and a message

        """
        request_fields = RequestDictionary.derive(self.request)
        try:
            if 'token' not in request_fields:
                raise ResponseException(
                    "Request body must include token", StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)
        token = request_fields['token']
        # Save token to be deleted after reset
        session["token"] = token
        success, message, errorCode = sesEmail.check_token(token, "password_reset")
        if success:
            #mark session that password can be filled out
            LoginSession.reset_password(session)

            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})

    def deleteUser(self):
        """ Deletes user specified by 'email' in request """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)
        try:
            if 'email' not in request_dict:
                # missing required fields, return 400
                raise ResponseException(
                    "Request body must include email of user to be deleted",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)
        email = request_dict['email']
        sess.query(User).filter(User.email == email).delete()
        sess.commit()
        return JsonResponse.create(StatusCode.OK,{"message":"success"})

    def update_user(self, system_email):
        """
        Update editable fields for specified user. Editable fields for a user:
        * is_active
        * user_status_id

        Args:
            system_email: address the email is sent from

        Request body should contain the following keys:
            * uid (integer)
            * status (string)
            * is_active (boolean)

        Returns: JSON response object with either an exception or success message

        """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)

        try:
            editable_fields = ('status', 'permissions', 'is_active')
            has_editable = any(key in request_dict for key in editable_fields)
            if 'uid' not in request_dict or not has_editable:
                # missing required fields, return 400
                raise ResponseException(
                    "Request body must include uid and at least one of the "
                    "following: status, permissions, is_active",
                    StatusCode.CLIENT_ERROR
                )
            # Find user that matches specified uid
            user = sess.query(User).filter_by(
                user_id=int(request_dict['uid'])).one_or_none()
            if user is None:
                raise ResponseException(
                    "No users with that uid", StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        if 'status' in request_dict:
            #check if the user is waiting
            if user.user_status_id == USER_STATUS_DICT["awaiting_approval"]:
                if request_dict['status'] == 'approved':
                    # Grant agency_user permission to newly approved users
                    user.permission_type_id = PERMISSION_TYPE_DICT['reader']
                    sess.merge(user)
                    sess.commit()

                    link = AccountHandler.FRONT_END
                    email_template = {'[URL]':link,'[EMAIL]':system_email}
                    new_email = sesEmail(user.email, system_email,templateType="account_approved",parameters=email_template)
                    new_email.send()
                elif request_dict['status'] == 'denied':
                    email_template = {}
                    new_email = sesEmail(user.email, system_email,templateType="account_rejected",parameters=email_template)
                    new_email.send()
            # Change user's status
            try:
                user.user_status_id = USER_STATUS_DICT[request_dict['status']]
            except ValueError as e:
                # In this case having a bad status name is a client error
                raise ResponseException(str(e), StatusCode.CLIENT_ERROR, ValueError)
            sess.commit()

        # Activate/deactivate user
        if 'is_active' in request_dict:
            is_active = bool(request_dict['is_active'])
            user.is_active = is_active
            sess.commit()

        return JsonResponse.create(StatusCode.OK, {"message": "User successfully updated"})

    def list_users(self):
        """ List all users ordered by status. Associated request body must have key 'filter_by' """
        request_dict = RequestDictionary.derive(
            self.request, optional_request=True)
        user_status = request_dict.get('status', 'all')
        sess = GlobalDB.db().session
        try:
            user_query = sess.query(User)
            if user_status != "all":
                user_query = user_query.filter(User.user_status_id == USER_STATUS_DICT[user_status])
            users = user_query.all()
        except ValueError as exc:
            # Client provided a bad status
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)
        user_info = []
        for user in users:
            agency_name = sess.query(CGAC.agency_name).\
                filter(CGAC.cgac_code == user.cgac_code).\
                one_or_none()

            thisInfo = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id, "is_active":user.is_active,
                        "permission": PERMISSION_TYPE_DICT_ID.get(user.permission_type_id), "status": user.user_status.name}
            user_info.append(thisInfo)
        return JsonResponse.create(StatusCode.OK,{"users":user_info})

    def list_user_emails(self):
        """ List user names and emails """
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == LoginSession.getName(flaskSession)).one()
        try:
            users = sess.query(User).filter(User.cgac_code == user.cgac_code, User.user_status_id == USER_STATUS_DICT["approved"], User.is_active == True).all()
        except ValueError as exc:
            # Client provided a bad status
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)
        user_info = []
        for user in users:
            this_info = {"id":user.user_id, "name": user.name, "email": user.email}
            user_info.append(this_info)
        return JsonResponse.create(StatusCode.OK, {"users": user_info})

    def list_users_with_status(self):
        """ List all users with the specified status.  Associated request body must have key 'status' """
        request_dict = RequestDictionary.derive(self.request)
        try:
            if 'status' not in request_dict:
                # Missing a required field, return 400
                raise ResponseException(
                    "Request body must include status", StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        sess = GlobalDB.db().session

        try:
            users = sess.query(User).filter_by(
                user_status_id=USER_STATUS_DICT[request_dict['status']]
            ).all()
        except ValueError as exc:
            # Client provided a bad status
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)
        user_info = []
        for user in users:
            agency_name = sess.query(CGAC.agency_name).\
                filter(CGAC.cgac_code == user.cgac_code).\
                one_or_none()
            this_info = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id }
            user_info.append(this_info)
        return JsonResponse.create(StatusCode.OK,{"users":user_info})

    def set_new_password(self, session):
        """ Set a new password for a user, request should have keys "user_email" and "password" """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)
        required = ('user_email', 'password')
        try:
            if any(field not in request_dict for field in required):
                # Don't have the keys we need in request
                raise ResponseException(
                    "Set password route requires keys user_email and password",
                    StatusCode.CLIENT_ERROR
                )
            if not self.checkPassword(request_dict['password']):
                raise ResponseException(
                    "Invalid Password", StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc,exc.status)

        # Get user from email
        user = sess.query(User).filter(
            func.lower(User.email) == func.lower(request_dict["user_email"])
        ).one()
        # Set new password
        set_user_password(user,request_dict["password"],self.bcrypt)
        # Invalidate token
        oldToken = sess.query(EmailToken).filter(EmailToken.token == session["token"]).one()
        sess.delete(oldToken)
        sess.commit()
        session["reset"] = None
        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password successfully changed"})

    def reset_password(self,system_email,session):
        """

        Remove old password and email user a token to set a new password.  Request should have key "email"

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask

        """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary.derive(self.request)
        try:
            if 'email' not in request_dict:
                # Don't have the keys we need in request
                raise ResponseException(
                    "Reset password route requires key 'email'",
                    StatusCode.CLIENT_ERROR
                )
            user = sess.query(User).filter(
                func.lower(User.email) == func.lower(request_dict['email'])
            ).one()
        except Exception as exc:
            return JsonResponse.error(exc, StatusCode.CLIENT_ERROR)

        email = request_dict['email']
        LoginSession.logout(session)
        self.send_reset_password_email(user, system_email, email)

        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password reset"})

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
        uid = session["name"]
        user = sess.query(User).filter(User.user_id == uid).one()
        agency_name = sess.query(CGAC.agency_name).\
            filter(CGAC.cgac_code == user.cgac_code).\
            one_or_none()
        return JsonResponse.create(StatusCode.OK, {
            "user_id": int(uid),
            "name": user.name,
            "agency_name": agency_name,
            "cgac_code": user.cgac_code,
            "title": user.title,
            "permission": user.permission_type_id,
            "skip_guide": user.skip_guide,
            "website_admin": user.website_admin
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
        user = sess.query(User).filter(User.user_id == session["name"]).one()
        request_dict = RequestDictionary.derive(self.request)
        try:
            if 'skip_guide' not in request_dict:
                raise ResponseException(
                    "Must include skip_guide parameter",
                    StatusCode.CLIENT_ERROR
                )
            skip_guide = request_dict['skip_guide']
            if isinstance(skip_guide, bool):    # e.g. from JSON
                user.skip_guide = skip_guide
            elif isinstance(skip_guide, str):
                # param is a string, allow "true" or "false"
                if skip_guide.lower() == "true":
                    user.skip_guide = True
                elif skip_guide.lower() == "false":
                    user.skip_guide = False
                else:
                    raise ResponseException(
                        "skip_guide must be true or false",
                        StatusCode.CLIENT_ERROR
                    )
            else:
                raise ResponseException(
                    "skip_guide must be a boolean", StatusCode.CLIENT_ERROR)
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

        current_user = sess.query(User).filter(User.user_id == session["name"]).one()

        user_ids = request_dict['users']
        submission_id = request_dict['submission_id']
        # Check if submission id is valid
        sess.query(Submission).filter_by(submission_id=submission_id).one()

        template_type = request_dict['email_template']
        # Check if email template type is valid
        get_email_template(template_type)

        users = []

        link = "".join([AccountHandler.FRONT_END, '#/reviewData/', str(submission_id)])
        email_template = {'[REV_USER_NAME]': current_user.name, '[REV_URL]': link}

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


def grant_highest_permission(user, group_list, cgac_group):
    """Find the highest permission within the provided cgac_group; set that as
    the user's permission_type_id"""
    user.cgac_code = cgac_group[-3:]
    user.website_admin = False
    permission_group = [g for g in group_list
                        if g.startswith(cgac_group + "-PERM_")]
    # Check if a user has been placed in a specific group. If not, deny access
    if not permission_group:
        user.permission_type_id = None
    else:
        perms = [perm[-1].lower() for perm in permission_group]
        ordered_perms = sorted(
            PERMISSION_MAP.items(), key=lambda pair: pair[1]['order'])
        for key, permission in ordered_perms:
            name = permission['name']
            if key in perms:
                user.permission_type_id = PERMISSION_TYPE_DICT[name]
                break
