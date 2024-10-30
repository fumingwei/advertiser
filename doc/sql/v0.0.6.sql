ALTER TABLE cu_tiktok_bc_accounts_details
ADD COLUMN business_id VARCHAR(32);

UPDATE cu_tiktok_bc_accounts_details
JOIN cu_tiktok_bc_accounts
ON cu_tiktok_bc_accounts_details.tiktok_bc_account_id = cu_tiktok_bc_accounts.id
SET cu_tiktok_bc_accounts_details.business_id = cu_tiktok_bc_accounts.business_id;

ALTER TABLE cu_tiktok_bc_accounts
DROP COLUMN business_id;