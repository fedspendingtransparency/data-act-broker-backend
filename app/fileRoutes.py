import flask
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.fileHandler import FileHandler
from handlers.jobHandler import JobHandler
from handlers.aws.session import LoginSession
from permissions import permissions_check
# Add the file submission route
def add_file_routes(app):
    """ Create routes related to file submission for flask app

    """
    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods = ["POST"])
    @permissions_check
    def submit_files():
        fileManager = FileHandler(request)
        return fileManager.submit(LoginSession.getName(session))


    @app.route("/v1/finalize_job/", methods = ["POST"])
    @permissions_check
    def finalize_submission() :
        fileManager = FileHandler(request)
        return fileManager.finalize()

    @app.route("/v1/check_status/", methods = ["POST"])
    @permissions_check
    def check_status():
        fileManager = FileHandler(request)
        return fileManager.getStatus()

    @app.route("/v1/submission_error_reports/", methods = ["POST"])
    @permissions_check
    def submission_error_reports() :
        fileManager = FileHandler(request)
        return fileManager.getErrorReportURLsForSubmission()
