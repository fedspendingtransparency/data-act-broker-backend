from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.userModel import EmailTemplateType, EmailTemplate
from dataactvalidator.health_check import create_app


def setup_emails():
    """Create email templates from model metadata."""
    with create_app().app_context():
        sess = GlobalDB.db().session

        # insert email template types
        type_list = [
            ('review_submission', '')
        ]
        for t in type_list:
            email_id = sess.query(
                EmailTemplateType.email_template_type_id).filter(
                EmailTemplateType.name == t[0]).one_or_none()
            if not email_id:
                email_type = EmailTemplateType(name=t[0], description=t[1])
                sess.add(email_type)

        sess.commit()

        # insert email templates

        # Submission Review
        template = ("[REV_USER_NAME] has shared a DATA Act broker submission with you from [REV_AGENCY]. Click "
                    "<a href='[REV_URL]'>here</a> to review their submission. For questions or comments, please visit "
                    "the Service Desk at https://servicedesk.usaspending.gov/ or e-mail DATAPMO@fiscal.treasury.gov.")
        load_email_template(sess, "DATA Act Broker - Submission Ready for Review", template, "review_submission")


def load_email_template(sess, subject, contents, email_type):
    """ Upsert a broker e-mail template.

    Args:
        sess - Database session
        subject - Subject line
        contents - Body of email, can include tags to be replaced
        email_type - Type of template, if there is already an entry for this type it will be overwritten
    """
    email_id = sess.query(
        EmailTemplateType.email_template_type_id).filter(
        EmailTemplateType.name == email_type).one()
    template_id = sess.query(
        EmailTemplate.email_template_id).filter(
        EmailTemplate.template_type_id == email_id).one_or_none()
    template = EmailTemplate()
    if template_id:
        template.email_template_id = template_id
    template.subject = subject
    template.content = contents
    template.template_type_id = email_id
    sess.merge(template)
    sess.commit()

if __name__ == '__main__':
    configure_logging()
    setup_emails()
