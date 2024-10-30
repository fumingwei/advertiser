# Gatherone Advertiser

Gatherone 客户自助系统

# 系统功能

(当前版本：V0.0.5)

* 账户开户
* 账户列表
* 账户充值
* 账户清零
* 余额转移
* 账户重命名
* BM绑定/解绑
* BC绑定/解绑
* Pixel绑定和解绑

# 部署

- Redis中添加api_key信息

    - Redis中设置PROD_API_WEB_ACCOUNT_SECRET(gatherone_advertiser_account_info)的api_key

        参数值示例:
        ```
        {
            "callback_url": "http://10.0.1.112:8010/api/v1/callback/on_complete",
            "app_name": "gatherone_advertiser"
        }
        ```

    - Redis中设置PROD_API_WEB_FINANCE_SECRET(gatherone_advertiser_balance_transfer)的api_key

        参数值示例:

        ```
        {
            "callback_url": "", # 因为不需要媒体API的回调，设置值为空字符串。
            "app_name": "gatherone_advertiser"
        }
        ```

- 启动

```
make deploy
```

- windows 启动celery
  - celery -A my_celery.main worker -Q update_accounts -n celery@advertiser_update_accounts --loglevel=info -P eventlet
  - celery -A my_celery.main worker -Q balance_transfer_refund -n celery@advertiser_balance_transfer_refund --loglevel=info -P eventlet
  - celery -A my_celery.main worker -Q balance_transfer_recharge -n celery@advertiser_balance_transfer_recharge --loglevel=info -P eventlet
  - celery -A my_celery.main beat --loglevel=info
  - celery -A my_celery.main worker -Q mapi_request_result -n celery@advertiser_mapi_request_result --loglevel=info -P eventlet

- 启动同步广告账户服务
  - python3 apps/common/mirroring_crm_account.py
