-- 添加账户转账媒介
ALTER TABLE cu_balance_transfers
ADD COLUMN medium varchar(100) NULL COMMENT '媒介';

-- 添加  转账类型
ALTER TABLE cu_balance_transfer_requests
ADD COLUMN transfer_type ENUM('1', '2') DEFAULT '1' COMMENT '转账类型';

-- 添加  转账类型 转出前钱包余额列  转出后钱包余额列
ALTER TABLE cu_balance_transfer_details
ADD COLUMN transfer_type ENUM('1', '2') DEFAULT '1' COMMENT '转账类型',
ADD COLUMN before_purse_balance DECIMAL(10, 2) NULL COMMENT '交易前钱包余额',
ADD COLUMN after_purse_balance DECIMAL(10, 2) NULL COMMENT '转出后钱包余额';

-- 添加账户转账媒介
ALTER TABLE gatherone_oms.cu_balance_transfers ADD medium varchar(100) NULL COMMENT '媒介';
-- 添加账户转账详情表转账类型
ALTER TABLE gatherone_oms.cu_balance_transfer_requests ADD transfer_type enum('1','2') NULL;
