def add_models(db, models=None):
    if models is None:
        models = []

    # Needs to happen in this order due to foreign key constraints between User and Submission
    for model in models:
        db.session.add(model)
        db.session.commit()


def delete_models(db, models=None):
    if models is None:
        models = []

    for model in models:
        db.session.delete(model)
    db.session.commit()
