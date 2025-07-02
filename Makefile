.PHONY: create-database destroy-database

create-database:
	@python ./pipeline/database_manager.py --create \
	--database-path ./air_quality.db \
	--ddl-query-parent-dir ./sql/ddl

destroy-database: 
	@python ./pipeline/database_manager.py --destroy \
	--database-path ./air_quality.db