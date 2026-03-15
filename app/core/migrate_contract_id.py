# migrate_contract_id.py
"""
数据迁移脚本：为现有报单和磅单填充 contract_id
"""
import pymysql
from database_setup import get_mysql_config

def migrate():
    config = get_mysql_config()
    conn = pymysql.connect(**config)
    
    try:
        with conn.cursor() as cur:
            # 1. 为 pd_deliveries 填充 contract_id
            cur.execute("""
                UPDATE pd_deliveries d
                LEFT JOIN pd_contracts c ON d.contract_no = c.contract_no
                SET d.contract_id = c.id
                WHERE d.contract_id IS NULL AND c.id IS NOT NULL
            """)
            print(f"Updated {cur.rowcount} deliveries with contract_id")
            
            # 2. 为 pd_weighbills 填充 contract_id
            cur.execute("""
                UPDATE pd_weighbills w
                LEFT JOIN pd_contracts c ON w.contract_no = c.contract_no
                SET w.contract_id = c.id
                WHERE w.contract_id IS NULL AND c.id IS NOT NULL
            """)
            print(f"Updated {cur.rowcount} weighbills with contract_id")
            
            # 3. 检查未匹配的数据（合同已删除的）
            cur.execute("""
                SELECT id, contract_no FROM pd_deliveries 
                WHERE contract_id IS NULL AND contract_no IS NOT NULL
            """)
            orphaned_deliveries = cur.fetchall()
            if orphaned_deliveries:
                print(f"Warning: {len(orphaned_deliveries)} deliveries with deleted contracts:")
                for d in orphaned_deliveries:
                    print(f"  - Delivery {d[0]}: contract_no={d[1]}")
            
            cur.execute("""
                SELECT id, contract_no FROM pd_weighbills 
                WHERE contract_id IS NULL AND contract_no IS NOT NULL
            """)
            orphaned_weighbills = cur.fetchall()
            if orphaned_weighbills:
                print(f"Warning: {len(orphaned_weighbills)} weighbills with deleted contracts:")
                for w in orphaned_weighbills:
                    print(f"  - Weighbill {w[0]}: contract_no={w[1]}")
            
            conn.commit()
            print("Migration completed")
            
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()