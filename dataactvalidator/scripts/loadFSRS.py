from sqlalchemy import func

from dataactcore.models.baseInterface import databaseSession
from dataactcore.models.fsrs import FSRSAward
from dataactvalidator.fsrs import retrieveAwardsBatch


def loadFSRS(minId):
    """Fetch and store a batch of Award/Sub-Award records, replacing previous
    entries."""
    awards = list(retrieveAwardsBatch(minId))
    ids = [a.id for a in awards]
    with databaseSession() as sess:
        sess.query(FSRSAward).filter(FSRSAward.id.in_(ids)).delete(
            synchronize_session=False)
        sess.add_all(awards)
        num_subawards = sum(len(a.subawards) for a in awards)
        sess.commit()
    return (len(ids), num_subawards)


def maxCurrentId():
    """We'll often want to load "new" data -- anything with a later id than
    the awards we have. Return that max id"""
    with databaseSession() as sess:
        maxId = sess.query(func.max(FSRSAward.id)).one()[0]
    return maxId or -1


if __name__ == '__main__':
    num_awards, num_subawards = loadFSRS(maxCurrentId() + 1)
    print("Inserted/Updated {} awards, {} subawards".format(
        num_awards, num_subawards))
