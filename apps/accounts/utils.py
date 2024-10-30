from apps.advertiser.models import (
    UserCusRelationship,
    AdvertiserUser
)


def get_customer_ids(db, user_id):
    user = (
        db.query(AdvertiserUser)
        .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
        .first()
    )
    relationship = (
        db.query(UserCusRelationship.customer_id)
        .filter(
            UserCusRelationship.company_id == user.company_id,
            UserCusRelationship.is_delete == False,
        )
        .first()
    )
    return relationship.customer_id
