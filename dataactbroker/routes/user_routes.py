from flask import g, request, session
from webargs import fields as webargs_fields
from webargs.flaskparser import use_kwargs

from dataactbroker.handlers.account_handler import (
    AccountHandler,
    json_for_user,
    list_user_emails,
    list_submission_users,
)

# from dataactbroker.decorators import convert_to_submission_id
from dataactbroker.permissions import requires_login  # requires_submission_perms
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


def add_user_routes(app, system_email, bcrypt):
    """Create routes related to user management

    Args:
        app - Flask app to add routes to
        system_email - Sender address to use for emails
        bcrypt - Password hashing Bcrypt associated with app
    """

    @app.route("/v1/list_user_emails/", methods=["GET"])
    @requires_login
    def list_user_emails_route():
        """list all users"""
        return list_user_emails()

    @app.route("/v1/list_submission_users/", methods=["GET"])
    @requires_login
    @use_kwargs({"is_fabs": webargs_fields.Bool(load_default=False)}, location="query")
    def list_submission_users_route(is_fabs):
        """List all users with submissions that the requesting user has permissions for"""
        return list_submission_users(is_fabs)

    @app.route("/v1/active_user/", methods=["GET"])
    @requires_login
    def active_user():
        """gets the current user information"""
        return JsonResponse.create(StatusCode.OK, json_for_user(g.user, session["sid"]))

    @app.route("/v1/set_skip_guide/", methods=["POST"])
    @requires_login
    @use_kwargs({"skip_guide": webargs_fields.Bool(required=True)}, location="json")
    def set_skip_guide(skip_guide):
        """Sets skip_guide param for current user"""
        account_manager = AccountHandler(request, bcrypt=bcrypt)
        return account_manager.set_skip_guide(skip_guide)

    # Commenting out due to issues with SES
    # @app.route("/v1/email_users/", methods=["POST"])
    # @convert_to_submission_id
    # @requires_submission_perms('reader')
    # @use_kwargs({
    #     'email_template': webargs_fields.String(required=True),
    #     'users': webargs_fields.List(webargs_fields.Int(), required=True)
    # }, location='json')
    # def email_users(submission, email_template, users):
    #     """ Sends email notifications to users that their submission is ready for review & publish viewing """
    #     account_manager = AccountHandler(request, bcrypt=bcrypt)
    #     return account_manager.email_users(submission, system_email, email_template, users)
