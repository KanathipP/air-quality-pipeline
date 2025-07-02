.PHONY: create-database destroy-database extract-data transform-data

create-database:
	@python ./pipeline/database_manager.py --create \
	--database_path ./air_quality.db \
	--ddl_query_parent_dir ./sql/ddl

destroy-database: 
	@python ./pipeline/database_manager.py --destroy \
	--database_path ./air_quality.db

extract-data:
	@python ./pipeline/extraction.py \
	--locations_file_path ./location.json \
	--start_date 2024-01 \
	--end_date 2024-02 \
	--extract_query_template_path ./sql/dml/raw/0_raw_air_quality_insert.sql \
	--database_path ./air_quality.db \
	--source_base_path s3://openaq-data-archive/records/csv.gz

transform-data:
	@python ./pipeline/transformation.py \
	--database_path ./air_quality.db \
	--query_directory ./sql/dml/presentation \