-- ----------------------------
2024-07-15
-- ----------------------------

ALTER TABLE cu_advertiser_registers
MODIFY COLUMN mobile VARCHAR(20) COMMENT '手机号';
UPDATE cu_advertiser_registers
SET mobile = CONCAT('+86 ', mobile);

ALTER TABLE cu_advertiser_users
MODIFY COLUMN mobile VARCHAR(20) COMMENT '手机号';
UPDATE cu_advertiser_users
SET mobile = CONCAT('+86 ', mobile);

ALTER TABLE cu_login_histories
MODIFY COLUMN mobile VARCHAR(20) COMMENT '手机号';
UPDATE cu_login_histories
SET mobile = CONCAT('+86 ', mobile);

ALTER TABLE cu_advertiser_registers
ADD COLUMN accredit_way ENUM('0', '1') NOT NULL DEFAULT '0' COMMENT '授权模式';