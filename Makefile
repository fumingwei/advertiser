web:
	python main.py

celery:
	docker-compose up celery_accounts_worker \
	celery_balance_transfer_refund_worker \
	celery_balance_transfer_recharge_worker \
	celery_beat \
	celery_mapi_request_result_worker

deploy:
	docker-compose up -d --build

ps:
	docker-compose ps