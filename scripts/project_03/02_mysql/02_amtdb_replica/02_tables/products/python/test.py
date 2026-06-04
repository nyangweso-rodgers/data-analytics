import os

print("MySQL DB:", os.getenv("amt_replica_mysql_db"))
print("MySQL User:", os.getenv("amt_replica_mysql_db_user"))
print("MySQL Password:", os.getenv("amt_replica_mysql_db_password"))
print("MySQL Host:", os.getenv("amt_replica_mysql_db_host"))
print("MySQL Port:", os.getenv("amt_replica_mysql_db_port"))