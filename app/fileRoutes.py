import flask
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.fileHandler import FileHandler
from handlers.jobHandler import JobHandler
from handlers.aws.session import LoginSession
# Add the file submission route
def add_file_routes(app):

    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods = ["POST"])
    def submit_files():
        response = flask.Response()
        fileManager = FileHandler(request,response)
        return fileManager.submit(LoginSession.getName(session))

    @app.route("/v1/finalize_submission/", methods = ["POST"])
    def finalize_submission() :
        response = flask.Response()
        fileManager = FileHandler(request,response)
        return fileManager.finalize()