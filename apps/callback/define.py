# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


class AdvertiserStatusResult(BaseEnum):
    DEFAULT = ('DEFAULT', '处理中')
    PART = ('PART', '部分成功')
    ALL_SUCCEED = ('ALL_SUCCEED', '全部成功')
    ALL_FAIL = ('ALL_FAIL', '全部失败')


class MediumOperation(BaseEnum):
    """
    对应的操作类型
    """
    Recharge = ('Recharge', 'Recharge')
    Reset = ('Reset', 'Reset')
    AccountRename = ('AccountRename', 'AccountRename')

    MetaAssetGroupAccountBind = ('MetaAssetGroupAccountBind', 'MetaAssetGroupAccountBind')
    MetaAssetGroupAccountUnBind = ('MetaAssetGroupAccountUnBind', 'MetaAssetGroupAccountUnBind')
    MetaAssetGroupUserBind = ('MetaAssetGroupUserBind', 'MetaAssetGroupUserBind')
    MetaAssetGroupUserUnBind = ('MetaAssetGroupUserUnBind', 'MetaAssetGroupUserUnBind')
    MetaPageUserBind = ('MetaPageUserBind', 'MetaPageUserBind')
    MetaPageUserUnBind = ('MetaPageUserUnBind', 'MetaPageUserUnBind')
    MetaBmAccountBind = ('MetaBmAccountBind', 'MetaBmAccountBind')
    MetaBmAccountUnBind = ('MetaBmAccountUnBind', 'MetaBmAccountUnBind')
    MetaUserAccountBind = ('MetaUserAccountBind', 'MetaUserAccountBind')
    MetaUserAccountUnBind = ('MetaUserAccountUnBind', 'MetaUserAccountUnBind')
    MetaAssetGroupBind = ('MetaAssetGroupBind', 'MetaAssetGroupBind')
    MetaAssetGroupUnBind = ('MetaAssetGroupUnBind', 'MetaAssetGroupUnBind')
    MetaBmBind = ('MetaBmBind', 'MetaBmBind')
    MetaBmUnBind = ('MetaBmUnBind', 'MetaBmUnBind')
    MetaPixelBind = ('MetaPixelBind', 'MetaPixelBind')
    MetaPixelUnBind = ('MetaPixelUnBind', 'MetaPixelUnBind')

    TiktokAdvertiserPartnerBind = ('TiktokAdvertiserPartnerBind', 'TiktokAdvertiserPartnerBind')
    TiktokAdvertiserPartnerUnBind = ('TiktokAdvertiserPartnerUnBind', 'TiktokAdvertiserPartnerUnBind')
    TiktokAdvertiserCampaignStatus = ('TiktokAdvertiserCampaignStatus', 'TiktokAdvertiserCampaignStatus')