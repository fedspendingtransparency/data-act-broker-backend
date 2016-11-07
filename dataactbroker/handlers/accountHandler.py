import re
import time
import requests
import xmltodict

from threading import Thread

from dateutil.parser import parse
from flask import session as flaskSession

from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.handlers.userHandler import UserHandler
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.interfaces.db import GlobalDB
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy import func
from dataactcore.models.userModel import User, PermissionType
from dataactcore.models.domainModels import CGAC
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.function_bag import sumNumberOfErrorsForJobList, getUsersByType
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import USER_STATUS_DICT

class AccountHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    FRONT_END = ""
    INACTIVITY_THRESHOLD = 120 # Days a user's account can be unused before being marked as inactive
    ALLOWED_PASSWORD_ATTEMPTS = 3 # Number of allowed login attempts before account is locked
    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request, interfaces = None, bcrypt = None):
        """ Creates the Login Handler

        Args:
            request - Flask request object
            interfaces - InterfaceHolder object for databases
            bcrypt - Bcrypt object associated with app
        """
        self.request = request
        self.bcrypt = bcrypt
        if(interfaces != None):
            self.interfaces = interfaces
            self.userManager = interfaces.userDb
            self.jobManager = interfaces.jobDb

    def addInterfaces(self,interfaces):
        """ Add interfaces to an existing account handler

        Args:
            interfaces - InterfaceHolder object for databases
        """
        self.interfaces = interfaces
        self.userManager = interfaces.userDb
        self.jobManager = interfaces.jobDb

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

            if not self.interfaces.userDb.checkStatus(user,"approved"):
                raise ValueError("Invalid username and/or password")

            # Only check if user is active after they've logged in for the first time
            if user.last_login_date is not None and self.isAccountExpired(user):
                raise ValueError("Your account has expired. Please contact an administrator.")

            # for whatever reason, your account is not active, therefore it's locked
            if not user.is_active:
                raise ValueError("Your account has been locked. Please contact an administrator.")

            try:
                if self.interfaces.userDb.checkPassword(user,password,self.bcrypt):
                    # We have a valid login

                    # Reset incorrect password attempt count to 0
                    self.resetPasswordCount(user)

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

            # Deny access if not in the parent group aka they're not allowed to access the website all together
            if not parent_group in group_list:
                raise ValueError("You have logged in with MAX but do not have permission to access the broker. Please "
                                 "contact DATABroker@fiscal.treasury.gov to obtain access.")

            cgac_group = [g for g in group_list if len(g) == len(parent_group + "-CGAC_")+3]

            # Deny access if they are not aligned with an agency
            if not cgac_group:
                raise ValueError("You have logged in with MAX but do not have permission to access the broker. Please "
                                 "contact DATABroker@fiscal.treasury.gov to obtain access.")

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

                    # If part of the SYS agency, use that as the cgac otherwise use the first agency provided
                    if [g for g in cgac_group if g.endswith("SYS")]:
                        user.cgac_code = "SYS"
                        # website admin permissions
                        UserHandler().grantPermission(user, 'website_admin')
                    else:
                        user.cgac_code = cgac_group[0][-3:]
                        # regular user permissions
                        UserHandler().grantPermission(user, 'agency_user')

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
        permission_list = []
        for permission in sess.query(PermissionType).all():
            if self.interfaces.userDb.hasPermission(user, permission.name):
                permission_list.append(permission.permission_type_id)
        self.interfaces.userDb.updateLastLogin(user)
        agency_name = sess.query(CGAC.agency_name).\
            filter(CGAC.cgac_code == user.cgac_code).\
            one_or_none()
        return JsonResponse.create(StatusCode.OK, {"message": "Login successful", "user_id": int(user.user_id),
                                                   "name": user.name, "title": user.title,
                                                   "agency_name": agency_name,
                                                   "cgac_code": user.cgac_code, "permissions": permission_list})

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
            threaded_database =  UserHandler()
            try:
                agency_name = sess.query(CGAC.agency_name).\
                    filter(CGAC.cgac_code == cgac_code).\
                    one_or_none()
                agency_name = "Unknown" if agency_name is None else agency_name
                for user in getUsersByType("website_admin"):
                    email_template = {'[REG_NAME]': username, '[REG_TITLE]':title, '[REG_AGENCY_NAME]':agency_name,
                                     '[REG_CGAC_CODE]': cgac_code,'[REG_EMAIL]' : user_email,'[URL]':link}
                    new_email = sesEmail(user.email, system_email,templateType="account_creation",parameters=email_template,database=threaded_database)
                    new_email.send()
                for user in getUsersByType("agency_admin"):
                    if user.cgac_code == cgac_code:
                        email_template = {'[REG_NAME]': username, '[REG_TITLE]': title, '[REG_AGENCY_NAME]': agency_name,
                             '[REG_CGAC_CODE]': cgac_code,'[REG_EMAIL]': user_email, '[URL]': link}
                        new_email = sesEmail(user.email, system_email, templateType="account_creation", parameters=email_template,
                                database=threaded_database)
                        new_email.send()

            finally:
                threaded_database.close()

        sess = GlobalDB.db().session
        request_fields = RequestDictionary(self.request)
        if not (request_fields.exists("email") and request_fields.exists("name") and request_fields.exists("cgac_code") and request_fields.exists("title") and request_fields.exists("password")):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include email, name, cgac_code, title, and password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if not self.checkPassword(request_fields.getValue("password")):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Find user that matches specified email
        user = sess.query(User).filter(func.lower(User.email) == func.lower(request_fields.getValue("email"))).one()
        # Check that user's status is before submission of registration
        if not (self.interfaces.userDb.checkStatus(user,"awaiting_confirmation") or self.interfaces.userDb.checkStatus(user,"email_confirmed")):
            # Do not allow duplicate registrations
            exc = ResponseException("User already registered",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Add user info to database
        self.interfaces.userDb.addUserInfo(user,request_fields.getValue("name"),request_fields.getValue("cgac_code"),request_fields.getValue("title"))
        self.interfaces.userDb.setPassword(user,request_fields.getValue("password"),self.bcrypt)

        user_link= "".join([AccountHandler.FRONT_END, '#/login?redirect=/admin'])
        # Send email to approver list
        email_thread = Thread(target=ThreadedFunction, kwargs=dict(username=user.name,title=user.title,cgac_code=user.cgac_code,user_email=user.email,link=user_link))
        email_thread.start()

        #email user
        email_template = {'[EMAIL]' : system_email}
        new_email = sesEmail(user.email, system_email,templateType="account_creation_user",parameters=email_template,database=self.interfaces.userDb)
        new_email.send()

        # Logout and delete token
        LoginSession.logout(session)
        self.interfaces.userDb.deleteToken(session["token"])
        # Mark user as awaiting approval
        self.interfaces.userDb.changeStatus(user,"awaiting_approval")
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def create_email_confirmation(self,system_email):
        """

        Creates user record and email

        arguments:

        system_email  -- (string) email used to send messages

        """
        sess = GlobalDB.db().session
        request_fields = RequestDictionary(self.request)
        if not request_fields.exists("email"):
            exc = ResponseException("Request body must include email", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        email = request_fields.getValue("email")
        if not re.match("[^@]+@[^@]+\.[^@]+",email):
            return JsonResponse.error(ValueError("Invalid Email Format"),StatusCode.CLIENT_ERROR)
        try :
            user = sess.query(User).filter(func.lower(User.email) == func.lower(request_fields.getValue("email"))).one()
        except NoResultFound:
            self.interfaces.userDb.addUnconfirmedEmail(email)
        else:
            if not (user.user_status_id == USER_STATUS_DICT["awaiting_confirmation"] or user.user_status_id == USER_STATUS_DICT["email_confirmed"]):
                exc = ResponseException("User already registered", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status)
        email_token = sesEmail.createToken(email, "validate_email")
        link= "".join([AccountHandler.FRONT_END,'#/registration/',email_token])
        email_template = {'[USER]': email, '[URL]':link}
        new_email = sesEmail(email, system_email,templateType="validate_email",parameters=email_template,database=self.interfaces.userDb)
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
        request_fields = RequestDictionary(self.request)
        if not request_fields.exists("token"):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        token = request_fields.getValue("token")
        session["token"] = token
        success,message,errorCode = sesEmail.checkToken(token,self.interfaces.userDb,"validate_email")
        if success:
            #mark session that email can be filled out
            LoginSession.register(session)

            #remove token so it cant be used again
            # The following line is commented out for issues with registration email links bouncing users back
            # to the original email input page instead of the registration page
            #self.interfaces.userDb.deleteToken(token)

            #set the status only if current status is awaiting confirmation
            user = sess.query(User).filter(func.lower(User.email) == func.lower(message)).one()
            if self.interfaces.userDb.checkStatus(user,"awaiting_confirmation"):
                self.interfaces.userDb.changeStatus(user,"email_confirmed")
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
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        token = requestFields.getValue("token")
        # Save token to be deleted after reset
        session["token"] = token
        success,message,errorCode = sesEmail.checkToken(token,self.interfaces.userDb,"password_reset")
        if(success):
            #mark session that password can be filled out
            LoginSession.reset_password(session)

            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})

    def deleteUser(self):
        """ Deletes user specified by 'email' in request """
        requestDict = RequestDictionary(self.request)
        if not requestDict.exists("email"):
            # missing required fields, return 400
            exc = ResponseException("Request body must include email of user to be deleted",
                                    StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)
        email = requestDict.getValue("email")
        self.interfaces.userDb.deleteUser(email)
        return JsonResponse.create(StatusCode.OK,{"message":"success"})

    def update_user(self, system_email):
        """
        Update editable fields for specified user. Editable fields for a user:
        * is_active
        * user_status_id
        * permissions

        Args:
            system_email: address the email is sent from

        Request body should contain the following keys:
            * uid (integer)
            * status (string)
            * permissions (comma separated string)
            * is_active (boolean)

        Returns: JSON response object with either an exception or success message

        """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary(self.request)

        # throw an exception if nothing is provided in the request
        if not request_dict.exists("uid") or not (request_dict.exists("status") or request_dict.exists("permissions") or
                    request_dict.exists("is_active")):
            # missing required fields, return 400
            exc = ResponseException("Request body must include uid and at least one of the following: status, permissions, is_active",
                                    StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)

        # Find user that matches specified uid
        user = sess.query(User).filter(User.user_id == int(request_dict.getValue("uid"))).one()

        if request_dict.exists("status"):
            #check if the user is waiting
            if self.interfaces.userDb.checkStatus(user,"awaiting_approval"):
                if request_dict.getValue("status") == "approved":
                    # Grant agency_user permission to newly approved users
                    self.interfaces.userDb.grantPermission(user,"agency_user")
                    link=  AccountHandler.FRONT_END
                    email_template = { '[URL]':link,'[EMAIL]':system_email}
                    new_email = sesEmail(user.email, system_email,templateType="account_approved",parameters=email_template,database=self.interfaces.userDb)
                    new_email.send()
                elif request_dict.getValue("status") == "denied":
                    email_template = {}
                    new_email = sesEmail(user.email, system_email,templateType="account_rejected",parameters=email_template,database=self.interfaces.userDb)
                    new_email.send()
            # Change user's status
            self.interfaces.userDb.changeStatus(user,request_dict.getValue("status"))

        if request_dict.exists("permissions"):
            permissions_list = request_dict.getValue("permissions").split(',')

            # Remove all existing permissions for user
            user_permissions = self.interfaces.userDb.getUserPermissions(user)
            for permission in user_permissions:
                self.interfaces.userDb.removePermission(user, permission)

            # Grant specified permissions
            for permission in permissions_list:
                self.interfaces.userDb.grantPermission(user, permission)

        # Activate/deactivate user
        if request_dict.exists("is_active"):
            is_active = bool(request_dict.getValue("is_active"))
            if not user.is_active and is_active:
                # Reset password count to 0
                self.resetPasswordCount(user)
                # Reset last login date so the account isn't expired
                self.interfaces.userDb.updateLastLogin(user, unlock_user=True)
                self.sendResetPasswordEmail(user, system_email, unlock_user=True)
            self.interfaces.userDb.setUserActive(user, is_active)

        return JsonResponse.create(StatusCode.OK, {"message": "User successfully updated"})

    def list_users(self):
        """ List all users ordered by status. Associated request body must have key 'filter_by' """
        request_dict = RequestDictionary(self.request, optionalRequest=True)
        user_status = request_dict.getValue("status") if request_dict.exists("status") else "all"
        sess = GlobalDB.db().session

        user = sess.query(User).filter(User.user_id == LoginSession.getName(flaskSession)).one()
        is_agency_admin = self.userManager.hasPermission(user, "agency_admin") and not self.userManager.hasPermission(user, "website_admin")
        try:
            if is_agency_admin:
                users = self.interfaces.userDb.getUsers(cgac_code=user.cgac_code, status=user_status)
            else:
                users = self.interfaces.userDb.getUsers(status=user_status)
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)
        user_info = []
        for user in users:
            agency_name = sess.query(CGAC.agency_name).\
                filter(CGAC.cgac_code == user.cgac_code).\
                one_or_none()
            thisInfo = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id, "is_active":user.is_active,
                        "permissions": ",".join(self.interfaces.userDb.getUserPermissions(user)), "status": user.user_status.name}
            user_info.append(thisInfo)
        return JsonResponse.create(StatusCode.OK,{"users":user_info})

    def list_user_emails(self):
        """ List user names and emails """
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == LoginSession.getName(flaskSession)).one()
        try:
            users = self.interfaces.userDb.getUsers(cgac_code=user.cgac_code, status="approved", only_active=True)
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, ValueError)
            return JsonResponse.error(exc, exc.status)
        user_info = []
        for user in users:
            this_info = {"id":user.user_id, "name": user.name, "email": user.email}
            user_info.append(this_info)
        return JsonResponse.create(StatusCode.OK, {"users": user_info})

    def list_users_with_status(self):
        """ List all users with the specified status.  Associated request body must have key 'status' """
        request_dict = RequestDictionary(self.request)
        if not (request_dict.exists("status")):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        sess = GlobalDB.db().session
        current_user = sess.query(User).filter(User.user_id == flaskSession["name"]).one()

        try:
            if self.interfaces.userDb.hasPermission(current_user, "agency_admin"):
                users = sess.query(User).filter(User.user_status_id == USER_STATUS_DICT[request_dict.getValue("status")], User.cgac_code == current_user.cgac_code).all()
            else:
                users = sess.query(User).filter(User.user_status_id == USER_STATUS_DICT[request_dict.getValue("status")]).all()
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)
        user_info = []
        for user in users:
            agency_name = sess.query(CGAC.agency_name).\
                filter(CGAC.cgac_code == user.cgac_code).\
                one_or_none()
            this_info = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id }
            user_info.append(this_info)
        return JsonResponse.create(StatusCode.OK,{"users":user_info})

    def list_submissions_by_current_user_agency(self):
        """ List all submission IDs associated with the current user's agency """
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == LoginSession.getName(flaskSession)).one()
        submissions = self.interfaces.jobDb.getSubmissionsByUserAgency(user)
        submission_details = []
        for submission in submissions:
            job_ids = self.interfaces.jobDb.getJobsBySubmission(submission.submission_id)
            total_size = 0
            for job_id in job_ids:
                file_size = self.interfaces.jobDb.getFileSize(job_id)
                total_size += file_size if file_size is not None else 0

            status = self.interfaces.jobDb.getSubmissionStatus(submission.submission_id)
            error_count = sumNumberOfErrorsForJobList(submission.submission_id)
            if submission.user_id is None:
                submission_user_name = "No user"
            else:
                submission_user_name = sess.query(User).filter(User.user_id == submission.user_id).one().name
            submission_details.append({"submission_id": submission.submission_id, "last_modified": submission.updated_at.strftime('%m/%d/%Y'),
                                      "size": total_size, "status": status, "errors": error_count, "reporting_start_date": str(submission.reporting_start_date),
                                      "reporting_end_date": str(submission.reporting_end_date), "user": {"user_id": submission.user_id,
                                                                                                    "name": submission_user_name}})
        return JsonResponse.create(StatusCode.OK, {"submissions": submission_details})

    def list_submissions_by_current_user(self):
        """ List all submission IDs associated with the current user ID """
        sess = GlobalDB.db().session
        user_id = LoginSession.getName(flaskSession)
        user = sess.query(User).filter(User.user_id == user_id).one()
        submissions = self.interfaces.jobDb.getSubmissionsByUserId(user_id)
        submission_details = []
        for submission in submissions:
            job_ids = self.interfaces.jobDb.getJobsBySubmission(submission.submission_id)
            total_size = 0
            for job_id in job_ids:
                file_size = self.interfaces.jobDb.getFileSize(job_id)
                total_size += file_size if file_size is not None else 0

            status = self.interfaces.jobDb.getSubmissionStatus(submission.submission_id)
            error_count = sumNumberOfErrorsForJobList(submission.submission_id)
            submission_details.append(
                {"submission_id": submission.submission_id, "last_modified": submission.updated_at.strftime('%m/%d/%Y'),
                 "size": total_size, "status": status, "errors": error_count, "reporting_start_date": str(submission.reporting_start_date),
                                      "reporting_end_date": str(submission.reporting_end_date), "user": {"user_id": str(user_id),
                                                                                                    "name": user.name}})
        return JsonResponse.create(StatusCode.OK, {"submissions": submission_details})

    def set_new_password(self, session):
        """ Set a new password for a user, request should have keys "user_email" and "password" """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary(self.request)
        if not (request_dict.exists("user_email") and request_dict.exists("password")):
            # Don't have the keys we need in request
            exc = ResponseException("Set password route requires keys user_email and password",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if not self.checkPassword(request_dict.getValue("password")):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user from email
        user = sess.query(User).filter(func.lower(User.email) == func.lower(request_dict.getValue("user_email"))).one()
        # Set new password
        self.interfaces.userDb.setPassword(user,request_dict.getValue("password"),self.bcrypt)
        # Invalidate token
        self.interfaces.userDb.deleteToken(session["token"])
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
        requestDict = RequestDictionary(self.request)
        if not (requestDict.exists("email")):
            # Don't have the keys we need in request
            exc = ResponseException("Reset password route requires key 'email'",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user object
        try:
            user = sess.query(User).filter(func.lower(User.email) == func.lower(requestDict.getValue("email"))).one()
        except Exception as e:
            exc = ResponseException("Unknown Error",StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)

        email = requestDict.getValue("email")
        LoginSession.logout(session)
        self.sendResetPasswordEmail(user, system_email, email)

        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password reset"})

    def sendResetPasswordEmail(self, user, system_email, email=None, unlock_user=False):
        if email is None:
            email = user.email

        # User must be approved and active to reset password
        if user.user_status_id != USER_STATUS_DICT["approved"]:
            raise ResponseException("User must be approved before resetting password", StatusCode.CLIENT_ERROR)
        elif not unlock_user and not user.is_active:
            raise ResponseException("User is locked, cannot reset password", StatusCode.CLIENT_ERROR)

        # If unlocking a user, wipe out current password
        if unlock_user:
            UserHandler().clearPassword(user)

        self.interfaces.userDb.session.commit()
        # Send email with token
        email_token = sesEmail.createToken(email, "password_reset")
        link = "".join([AccountHandler.FRONT_END, '#/forgotpassword/', email_token])
        email_template = {'[URL]': link}
        template_type = "unlock_account" if unlock_user else "reset_password"
        new_email = sesEmail(user.email, system_email, templateType=template_type,
                            parameters=email_template, database=self.interfaces.userDb)
        new_email.send()

    def get_current_user(self,session):
        """

        Gets the current user information

        arguments:

        session  -- (Session) object from flask

        return the response object with the current user information

        """
        sess = GlobalDB.db().session
        uid =  session["name"]
        user = sess.query(User).filter(User.user_id == uid).one()
        permission_list = []
        for permission in sess.query(PermissionType).all():
            if self.interfaces.userDb.hasPermission(user, permission.name):
                permission_list.append(permission.permission_type_id)
        agency_name = sess.query(CGAC.agency_name).\
            filter(CGAC.cgac_code == user.cgac_code).\
            one_or_none()
        return JsonResponse.create(StatusCode.OK,{"user_id": int(uid),"name":user.name,"agency_name": agency_name,
                                                  "cgac_code":user.cgac_code,"title":user.title,
                                                  "permissions": permission_list, "skip_guide":user.skip_guide})

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

    def resetPasswordCount(self, user):
        """ Resets the number of failed attempts when a user successfully logs in

        Args:
            user: User object to be changed
        """
        if user.incorrect_password_attempts != 0:
            user.incorrect_password_attempts = 0
            self.interfaces.userDb.session.commit()

    def incrementPasswordCount(self, user):
        """ Records a failed attempt to log in.  If number of failed attempts is higher than threshold, locks account.

        Args:
            user: User object to be changed

        Returns:

        """
        if user.incorrect_password_attempts < self.ALLOWED_PASSWORD_ATTEMPTS:
            user.incorrect_password_attempts += 1
            if user.incorrect_password_attempts == self.ALLOWED_PASSWORD_ATTEMPTS:
                self.lockAccount(user)
            self.interfaces.userDb.session.commit()

    def lockAccount(self, user):
        """ Lock this user's account by marking it as inactive

        Args:
            user: User object to be locked
        """
        user.is_active = False
        self.interfaces.userDb.session.commit()

    def set_skip_guide(self, session):
        """ Set current user's skip guide parameter """
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == session["name"]).one()
        request_dict = RequestDictionary(self.request)
        if not request_dict.exists("skip_guide"):
            exc = ResponseException("Must include skip_guide parameter", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)
        skip_guide = request_dict.getValue("skip_guide")
        if type(skip_guide) == type(True):
            # param is a bool
            user.skip_guide = skip_guide
        elif type(skip_guide) == type("string"):
            # param is a string, allow "true" or "false"
            if skip_guide.lower() == "true":
                user.skip_guide = True
            elif skip_guide.lower() == "false":
                user.skip_guide = False
            else:
                exc = ResponseException("skip_guide must be true or false", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc, exc.status)
        else:
            exc = ResponseException("skip_guide must be a boolean", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)
        sess.commit()
        return JsonResponse.create(StatusCode.OK,{"message":"skip_guide set successfully","skip_guide":skip_guide})

    def email_users(self, system_email, session):
        """ Send email notification to list of users """
        sess = GlobalDB.db().session
        request_dict = RequestDictionary(self.request)
        if not (request_dict.exists("users") and request_dict.exists("submission_id") and request_dict.exists("email_template")):
            exc = ResponseException("Email users route requires users, email_template, and submission_id", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)

        current_user = sess.query(User).filter(User.user_id == session["name"]).one()

        user_ids = request_dict.getValue("users")
        submission_id = request_dict.getValue("submission_id")
        # Check if submission id is valid
        self.jobManager.getSubmissionById(submission_id)

        template_type = request_dict.getValue("email_template")
        # Check if email template type is valid
        self.userManager.getEmailTemplate(template_type)

        users = []

        link = "".join([AccountHandler.FRONT_END, '#/reviewData/', str(submission_id)])
        email_template = {'[REV_USER_NAME]': current_user.name, '[REV_URL]': link}

        for user_id in user_ids:
            # Check if user id is valid, if so add User object to array
            users.append(sess.query(User).filter(User.user_id == user_id).one())

        for user in users:
            new_email = sesEmail(user.email, system_email, templateType=template_type, parameters=email_template,
                            database=UserHandler())
            new_email.send()

        return JsonResponse.create(StatusCode.OK, {"message": "Emails successfully sent"})
