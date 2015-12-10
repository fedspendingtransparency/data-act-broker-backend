import flask

from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash ,session, Response
import json
from handlers.validationManager import ValidationManager

debugFlag = True

# Create application
app = Flask(__name__)
app.config.from_object(__name__)

validationManager = ValidationManager()

@app.route("/validate/",methods=["POST"])
def validate():
    return validationManager.validateJob(request)

if __name__ == '__main__':
    app.run(debug=debugFlag)