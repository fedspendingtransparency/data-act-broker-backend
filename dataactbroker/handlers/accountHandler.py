import re
import time
from threading import Thread

from dateutil.parser import parse
from flask import session as flaskSession

from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.handlers.userHandler import UserHandler
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


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
            self.validationManager = interfaces.validationDb
            self.jobManager = interfaces.jobDb

    def addInterfaces(self,interfaces):
        """ Add interfaces to an existing account handler

        Args:
            interfaces - InterfaceHolder object for databases
        """
        self.interfaces = interfaces
        self.userManager = interfaces.userDb
        self.validationManager = interfaces.validationDb
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

        return the reponse object

        """
        try:
            safeDictionary = RequestDictionary(self.request)

            username = safeDictionary.getValue('username')

            password = safeDictionary.getValue('password')

            try:
                user = self.interfaces.userDb.getUserByEmail(username)
            except Exception as e:
                raise ValueError("Invalid username and/or password")

            if(not self.interfaces.userDb.checkStatus(user,"approved")):
                raise ValueError("Invalid username and/or password")

            # Only check if user is active after they've logged in for the first time
            if user.last_login_date is not None and self.isAccountExpired(user):
                raise ValueError("Your account has expired. Please contact an administrator.")

            # for whatever reason, your account is not active, therefore it's locked
            if not self.isUserActive(user):
                raise ValueError("Your account has been locked. Please contact an administrator.")

            try:
                if(self.interfaces.userDb.checkPassword(user,password,self.bcrypt)):
                    # We have a valid login

                    # Reset incorrect password attempt count to 0
                    self.resetPasswordCount(user)

                    LoginSession.login(session,user.user_id)
                    permissionList = []
                    for permission in self.interfaces.userDb.getPermissionList():
                        if(self.interfaces.userDb.hasPermission(user, permission.name)):
                            permissionList.append(permission.permission_type_id)
                    self.interfaces.userDb.updateLastLogin(user)
                    agency_name = self.interfaces.validationDb.getAgencyName(user.cgac_code)
                    return JsonResponse.create(StatusCode.OK,{"message":"Login successful","user_id": int(user.user_id),
                                                              "name":user.name,"title":user.title,"agency_name":agency_name,
                                                              "cgac_code":user.cgac_code, "permissions" : permissionList})
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
                raise ValueError("Invalid username and/or password")

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
        def ThreadedFunction (from_email="",username="",title="",cgac_code="",userEmail="" ,link="") :
            """
            This inner function sends emails in a new thread as there could be lots of admins

            from_email -- (string) the from email address
            username -- (string) the name of the  user
            title  --   (string) the title of the  user
            agency -- (string) the agency of the  user
            userEmail -- (string) the email of the user
            link  -- (string) the broker email link
            """
            threadedDatabase =  UserHandler()
            try:
                agency_name = self.interfaces.validationDb.getAgencyName(cgac_code)
                agency_name = "Unknown" if agency_name is None else agency_name
                for user in threadedDatabase.getUsersByType("website_admin"):
                    emailTemplate = {'[REG_NAME]': username, '[REG_TITLE]':title, '[REG_AGENCY_NAME]':agency_name,
                                     '[REG_CGAC_CODE]': cgac_code,'[REG_EMAIL]' : userEmail,'[URL]':link}
                    newEmail = sesEmail(user.email, system_email,templateType="account_creation",parameters=emailTemplate,database=threadedDatabase)
                    newEmail.send()
                for user in threadedDatabase.getUsersByType("agency_admin"):
                    if user.cgac_code == cgac_code:
                        emailTemplate = {'[REG_NAME]': username, '[REG_TITLE]': title, '[REG_AGENCY_NAME]': agency_name,
                             '[REG_CGAC_CODE]': cgac_code,'[REG_EMAIL]': userEmail, '[URL]': link}
                        newEmail = sesEmail(user.email, system_email, templateType="account_creation", parameters=emailTemplate,
                                database=threadedDatabase)
                        newEmail.send()

            finally:
                threadedDatabase.close()

        requestFields = RequestDictionary(self.request)
        if(not (requestFields.exists("email") and requestFields.exists("name") and requestFields.exists("cgac_code") and requestFields.exists("title") and requestFields.exists("password"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include email, name, cgac_code, title, and password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if(not self.checkPassword(requestFields.getValue("password"))):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        # Check that user's status is before submission of registration
        if not (self.interfaces.userDb.checkStatus(user,"awaiting_confirmation") or self.interfaces.userDb.checkStatus(user,"email_confirmed")):
            # Do not allow duplicate registrations
            exc = ResponseException("User already registered",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Add user info to database
        self.interfaces.userDb.addUserInfo(user,requestFields.getValue("name"),requestFields.getValue("cgac_code"),requestFields.getValue("title"))
        self.interfaces.userDb.setPassword(user,requestFields.getValue("password"),self.bcrypt)

        userLink= "".join([AccountHandler.FRONT_END, '#/login?redirect=/admin'])
        # Send email to approver list
        emailThread = Thread(target=ThreadedFunction, kwargs=dict(from_email=system_email,username=user.name,title=user.title,cgac_code=user.cgac_code,userEmail=user.email,link=userLink))
        emailThread.start()

        #email user
        link= AccountHandler.FRONT_END
        emailTemplate = {'[EMAIL]' : system_email}
        newEmail = sesEmail(user.email, system_email,templateType="account_creation_user",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()

        # Logout and delete token
        LoginSession.logout(session)
        self.interfaces.userDb.deleteToken(session["token"])
        # Mark user as awaiting approval
        self.interfaces.userDb.changeStatus(user,"awaiting_approval")
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def createEmailConfirmation(self,system_email,session):
        """

        Creates user record and email

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask

        """
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("email")):
            exc = ResponseException("Request body must include email", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        email = requestFields.getValue("email")
        if( not re.match("[^@]+@[^@]+\.[^@]+",email)) :
            return JsonResponse.error(ValueError("Invalid Email Format"),StatusCode.CLIENT_ERROR)
        try :
            user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        except ResponseException as e:
            self.interfaces.userDb.addUnconfirmedEmail(email)
        else:
            if(not (user.user_status_id == self.interfaces.userDb.getUserStatusId("awaiting_confirmation") or user.user_status_id == self.interfaces.userDb.getUserStatusId("email_confirmed"))):
                exc = ResponseException("User already registered", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status)
        emailToken = sesEmail.createToken(email, "validate_email")
        link= "".join([AccountHandler.FRONT_END,'#/registration/',emailToken])
        emailTemplate = {'[USER]': email, '[URL]':link}
        newEmail = sesEmail(email, system_email,templateType="validate_email",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()
        return JsonResponse.create(StatusCode.OK,{"message":"Email Sent"})

    def checkEmailConfirmationToken(self,session):
        """

        Creates user record and email

        arguments:

        session -- (Session) object from flask

        return the reponse object with a error code and a message

        """
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        token = requestFields.getValue("token")
        session["token"] = token
        success,message,errorCode = sesEmail.checkToken(token,self.interfaces.userDb,"validate_email")
        if(success):
            #mark session that email can be filled out
            LoginSession.register(session)

            #remove token so it cant be used again
            # The following line is commented out for issues with registration email links bouncing users back
            # to the original email input page instead of the registration page
            #self.interfaces.userDb.deleteToken(token)

            #set the status only if current status is awaiting confirmation
            user = self.interfaces.userDb.getUserByEmail(message)
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
            LoginSession.resetPassword(session)

            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})

    def updateUser(self, system_email):
        """
        Update editable fields for specified user. Editable fields for a user:
        * is_active
        * user_status_id
        * permissions

        Args:
            None: Request body should contain the following keys:
                * uid (integer)
                * status (string)
                * permissions (comma separated string)
                * is_active (boolean)

        Returns: JSON response object with either an exception or success message

        """
        requestDict = RequestDictionary(self.request)

        # throw an exception if nothing is provided in the request
        if not requestDict.exists("uid") or not (requestDict.exists("status") or requestDict.exists("permissions") or
                    requestDict.exists("is_active")):
            # missing required fields, return 400
            exc = ResponseException("Request body must include uid and at least one of the following: status, permissions, is_active",
                                    StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)

        # Find user that matches specified uid
        user = self.interfaces.userDb.getUserByUID(int(requestDict.getValue("uid")))

        if requestDict.exists("status"):
            #check if the user is waiting
            if(self.interfaces.userDb.checkStatus(user,"awaiting_approval")):
                if(requestDict.getValue("status") == "approved"):
                    # Grant agency_user permission to newly approved users
                    self.interfaces.userDb.grantPermission(user,"agency_user")
                    link=  AccountHandler.FRONT_END
                    emailTemplate = { '[URL]':link,'[EMAIL]':system_email}
                    newEmail = sesEmail(user.email, system_email,templateType="account_approved",parameters=emailTemplate,database=self.interfaces.userDb)
                    newEmail.send()
                elif (requestDict.getValue("status") == "denied"):
                    emailTemplate = {}
                    newEmail = sesEmail(user.email, system_email,templateType="account_rejected",parameters=emailTemplate,database=self.interfaces.userDb)
                    newEmail.send()
            # Change user's status
            self.interfaces.userDb.changeStatus(user,requestDict.getValue("status"))

        if requestDict.exists("permissions"):
            permissions_list = requestDict.getValue("permissions").split(',')

            # Remove all existing permissions for user
            user_permissions = self.interfaces.userDb.getUserPermissions(user)
            for permission in user_permissions:
                self.interfaces.userDb.removePermission(user, permission)

            # Grant specified permissions
            for permission in permissions_list:
                self.interfaces.userDb.grantPermission(user, permission)

        # Activate/deactivate user
        if requestDict.exists("is_active"):
            is_active = bool(requestDict.getValue("is_active"))
            if not self.isUserActive(user) and is_active:
                # Reset password count to 0
                self.resetPasswordCount(user)
                # Reset last login date so the account isn't expired
                self.interfaces.userDb.updateLastLogin(user, unlock_user=True)
                self.sendResetPasswordEmail(user, system_email, unlock_user=True)
            self.interfaces.userDb.setUserActive(user, is_active)

        return JsonResponse.create(StatusCode.OK, {"message": "User successfully updated"})

    def changeStatus(self,system_email):
        """

        Changes status for specified user.  Associated request body should have keys 'uid' and 'new_status'

        arguments:

        system_email  -- (string) the emaily to send emails from

        return the reponse object with a success message

        """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("uid") and requestDict.exists("new_status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include uid and new_status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        # Find user that matches specified uid
        user = self.interfaces.userDb.getUserByUID(int(requestDict.getValue("uid")))

        if(user.email == None):
            return JsonResponse.error(ResponseException("User does not have a defined email",StatusCode.INTERNAL_ERROR),StatusCode.INTERNAL_ERROR)

        #check if the user is waiting
        if(self.interfaces.userDb.checkStatus(user,"awaiting_approval")):
            if(requestDict.getValue("new_status") == "approved"):
                # Grant agency_user permission to newly approved users
                self.interfaces.userDb.grantPermission(user,"agency_user")
                link=  AccountHandler.FRONT_END
                emailTemplate = { '[URL]':link,'[EMAIL]':system_email}
                newEmail = sesEmail(user.email, system_email,templateType="account_approved",parameters=emailTemplate,database=self.interfaces.userDb)
                newEmail.send()
            elif (requestDict.getValue("new_status") == "denied"):
                emailTemplate = {}
                newEmail = sesEmail(user.email, system_email,templateType="account_rejected",parameters=emailTemplate,database=self.interfaces.userDb)
                newEmail.send()
        # Change user's status
        self.interfaces.userDb.changeStatus(user,requestDict.getValue("new_status"))
        return JsonResponse.create(StatusCode.OK,{"message":"Status change successful"})

    def listUsers(self):
        """ List all users ordered by status. Associated request body must have key 'filter_by' """
        requestDict = RequestDictionary(self.request, optionalRequest=True)
        user_status = requestDict.getValue("status") if requestDict.exists("status") else "all"

        user = self.interfaces.userDb.getUserByUID(LoginSession.getName(flaskSession))
        isAgencyAdmin = self.userManager.hasPermission(user, "agency_admin") and not self.userManager.hasPermission(user, "website_admin")
        try:
            if isAgencyAdmin:
                users = self.interfaces.userDb.getUsers(cgac_code=user.cgac_code, status=user_status)
            else:
                users = self.interfaces.userDb.getUsers(status=user_status)
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)
        userInfo = []
        for user in users:
            agency_name = self.interfaces.validationDb.getAgencyName(user.cgac_code)
            thisInfo = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id, "is_active":user.is_active,
                        "permissions": ",".join(self.interfaces.userDb.getUserPermissions(user)), "status": user.user_status.name}
            userInfo.append(thisInfo)
        return JsonResponse.create(StatusCode.OK,{"users":userInfo})

    def listUserEmails(self):
        """ List user names and emails """

        user = self.interfaces.userDb.getUserByUID(LoginSession.getName(flaskSession))
        try:
            users = self.interfaces.userDb.getUsers(cgac_code=user.cgac_code, status="approved", only_active=True)
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, ValueError)
            return JsonResponse.error(exc, exc.status)
        userInfo = []
        for user in users:
            thisInfo = {"id":user.user_id, "name": user.name, "email": user.email}
            userInfo.append(thisInfo)
        return JsonResponse.create(StatusCode.OK, {"users": userInfo})

    def listUsersWithStatus(self):
        """ List all users with the specified status.  Associated request body must have key 'status' """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        current_user = self.interfaces.userDb.getUserByUID(flaskSession["name"])

        try:
            if self.interfaces.userDb.hasPermission(current_user, "agency_admin"):
                users = self.interfaces.userDb.getUsersByStatus(requestDict.getValue("status"), current_user.cgac_code)
            else:
                users = self.interfaces.userDb.getUsersByStatus(requestDict.getValue("status"))
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)
        userInfo = []
        for user in users:
            agency_name = self.interfaces.validationDb.getAgencyName(user.cgac_code)
            thisInfo = {"name":user.name, "title":user.title, "agency_name":agency_name, "cgac_code":user.cgac_code,
                        "email":user.email, "id":user.user_id }
            userInfo.append(thisInfo)
        return JsonResponse.create(StatusCode.OK,{"users":userInfo})

    def listSubmissionsByCurrentUserAgency(self):
        """ List all submission IDs associated with the current user's agency """
        userId = LoginSession.getName(flaskSession)
        user = self.interfaces.userDb.getUserByUID(userId)
        submissions = self.interfaces.jobDb.getSubmissionsByUserAgency(user)
        submissionDetails = []
        for submission in submissions:
            jobIds = self.interfaces.jobDb.getJobsBySubmission(submission.submission_id)
            total_size = 0
            for jobId in jobIds:
                file_size = self.interfaces.jobDb.getFileSize(jobId)
                total_size += file_size if file_size is not None else 0

            status = self.interfaces.jobDb.getSubmissionStatus(submission.submission_id, self.interfaces)
            error_count = self.interfaces.errorDb.sumNumberOfErrorsForJobList(jobIds, self.interfaces.validationDb)
            submission_user_name = self.interfaces.userDb.getUserByUID(submission.user_id).name
            submissionDetails.append({"submission_id": submission.submission_id, "last_modified": submission.updated_at.strftime('%m/%d/%Y'),
                                      "size": total_size, "status": status, "errors": error_count, "reporting_start_date": str(submission.reporting_start_date),
                                      "reporting_end_date": str(submission.reporting_end_date), "user": {"user_id": submission.user_id,
                                                                                                    "name": submission_user_name}})
        return JsonResponse.create(StatusCode.OK, {"submissions": submissionDetails})

    def listSubmissionsByCurrentUser(self):
        """ List all submission IDs associated with the current user ID """
        userId = LoginSession.getName(flaskSession)
        user = self.interfaces.userDb.getUserByUID(userId)
        submissions = self.interfaces.jobDb.getSubmissionsByUserId(userId)
        submissionDetails = []
        for submission in submissions:
            jobIds = self.interfaces.jobDb.getJobsBySubmission(submission.submission_id)
            total_size = 0
            for jobId in jobIds:
                file_size = self.interfaces.jobDb.getFileSize(jobId)
                total_size += file_size if file_size is not None else 0

            status = self.interfaces.jobDb.getSubmissionStatus(submission.submission_id, self.interfaces)
            error_count = self.interfaces.errorDb.sumNumberOfErrorsForJobList(jobIds, self.interfaces.validationDb)
            submissionDetails.append(
                {"submission_id": submission.submission_id, "last_modified": submission.updated_at.strftime('%m/%d/%Y'),
                 "size": total_size, "status": status, "errors": error_count, "reporting_start_date": str(submission.reporting_start_date),
                                      "reporting_end_date": str(submission.reporting_end_date), "user": {"user_id": str(userId),
                                                                                                    "name": user.name}})
        return JsonResponse.create(StatusCode.OK, {"submissions": submissionDetails})

    def setNewPassword(self, session):
        """ Set a new password for a user, request should have keys "user_email" and "password" """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("user_email") and requestDict.exists("password"))):
            # Don't have the keys we need in request
            exc = ResponseException("Set password route requires keys user_email and password",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if(not self.checkPassword(requestDict.getValue("password"))):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user from email
        user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("user_email"))
        # Set new password
        self.interfaces.userDb.setPassword(user,requestDict.getValue("password"),self.bcrypt)
        # Invalidate token
        self.interfaces.userDb.deleteToken(session["token"])
        session["reset"] = None
        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password successfully changed"})

    def resetPassword(self,system_email,session):
        """

        Remove old password and email user a token to set a new password.  Request should have key "email"

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask

        """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("email"))):
            # Don't have the keys we need in request
            exc = ResponseException("Reset password route requires key 'email'",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user object
        try:
            user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("email"))
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
        if user.user_status_id != self.interfaces.userDb.getUserStatusId("approved"):
            raise ResponseException("User must be approved before resetting password", StatusCode.CLIENT_ERROR)
        elif not unlock_user and not user.is_active:
            raise ResponseException("User is locked, cannot reset password", StatusCode.CLIENT_ERROR)

        # If unlocking a user, wipe out current password
        if unlock_user:
            UserHandler().clearPassword(user)

        self.interfaces.userDb.session.commit()
        # Send email with token
        emailToken = sesEmail.createToken(email, "password_reset")
        link = "".join([AccountHandler.FRONT_END, '#/forgotpassword/', emailToken])
        emailTemplate = {'[URL]': link}
        templateType = "unlock_account" if unlock_user else "reset_password"
        newEmail = sesEmail(user.email, system_email, templateType=templateType,
                            parameters=emailTemplate, database=self.interfaces.userDb)
        newEmail.send()

    def getCurrentUser(self,session):
        """

        Gets the current user information

        arguments:

        session  -- (Session) object from flask

        return the reponse object with the current user information

        """
        uid =  session["name"]
        user =  self.interfaces.userDb.getUserByUID(uid)
        permissionList = []
        for permission in self.interfaces.userDb.getPermissionList():
            if(self.interfaces.userDb.hasPermission(user, permission.name)):
                permissionList.append(permission.permission_type_id)
        agency_name = self.interfaces.validationDb.getAgencyName(user.cgac_code)
        return JsonResponse.create(StatusCode.OK,{"user_id": int(uid),"name":user.name,"agency_name": agency_name,
                                                  "cgac_code":user.cgac_code,"title":user.title,
                                                  "permissions": permissionList, "skip_guide":user.skip_guide})

    def isUserActive(self, user):
        """ Checks if user's account is still active

        Args:
            user: User object to check
        """
        return user.is_active

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

    def setSkipGuide(self, session):
        """ Set current user's skip guide parameter """
        uid =  session["name"]
        userDb = self.interfaces.userDb
        user =  userDb.getUserByUID(uid)
        requestDict = RequestDictionary(self.request)
        if not requestDict.exists("skip_guide"):
            exc = ResponseException("Must include skip_guide parameter", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)
        skipGuide = requestDict.getValue("skip_guide")
        if type(skipGuide) == type(True):
            # param is a bool
            user.skip_guide = skipGuide
        elif type(skipGuide) == type("string"):
            # param is a string, allow "true" or "false"
            if skipGuide.lower() == "true":
                user.skip_guide = True
            elif skipGuide.lower() == "false":
                user.skip_guide = False
            else:
                exc = ResponseException("skip_guide must be true or false", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc, exc.status)
        else:
            exc = ResponseException("skip_guide must be a boolean", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)
        userDb.session.commit()
        return JsonResponse.create(StatusCode.OK,{"message":"skip_guide set successfully","skip_guide":skipGuide})

    def emailUsers(self, system_email, session):
        """ Send email notification to list of users """
        requestDict = RequestDictionary(self.request)
        if not (requestDict.exists("users") and requestDict.exists("submission_id") and requestDict.exists("email_template")):
            exc = ResponseException("Email users route requires users, email_template, and submission_id", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc, exc.status)

        uid = session["name"]
        current_user = self.interfaces.userDb.getUserByUID(uid)

        user_ids = requestDict.getValue("users")
        submission_id = requestDict.getValue("submission_id")
        # Check if submission id is valid
        self.jobManager.getSubmissionById(submission_id)

        template_type = requestDict.getValue("email_template")
        # Check if email template type is valid
        self.userManager.getEmailTemplate(template_type)

        users = []

        link = "".join([AccountHandler.FRONT_END, '#/reviewData/', str(submission_id)])
        emailTemplate = {'[REV_USER_NAME]': current_user.name, '[REV_URL]': link}

        for user_id in user_ids:
            # Check if user id is valid, if so add User object to array
            users.append(self.userManager.getUserByUID(user_id))

        for user in users:
            newEmail = sesEmail(user.email, system_email, templateType=template_type, parameters=emailTemplate,
                            database=UserHandler())
            newEmail.send()

        return JsonResponse.create(StatusCode.OK, {"message": "Emails successfully sent"})
