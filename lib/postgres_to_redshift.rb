require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require 'time'
require 'postgres_to_redshift/table'
require 'postgres_to_redshift/column'
require 'postgres_to_redshift/copy_import'
require 'postgres_to_redshift/full_import'
require 'postgres_to_redshift/incremental_import'
require 'postgres_to_redshift/version'

module PostgresToRedshift
  TIMESTAMP_FILE_NAME = 'POSTGRES_TO_REDHSIFT_TIMESTAMP'.freeze

  def self.update_tables
    track_incremental do |incremental_from|
      tables.each do |table|
        CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema, incremental_from: incremental_from).run
      end
    end
  end

  def self.incremental?
    ENV['POSTGRES_TO_REDSHIFT_INCREMENTAL'] == 'true' && File.exist?(TIMESTAMP_FILE_NAME)
  end

  def self.track_incremental
    start_time = Time.now.utc
    incremental_from = incremental? ? Time.parse(File.read(TIMESTAMP_FILE_NAME)).utc : nil

    target_connection.exec('BEGIN;') if incremental?

    yield incremental_from

    if incremental?
      if PostgresToRedshift.dry_run?
        target_connection.exec('ROLLBACK;')
      else
        target_connection.exec('COMMIT;')
      end
    end

    File.write(TIMESTAMP_FILE_NAME, start_time.iso8601)
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
    end

    @source_connection
  end

  def self.target_connection
    @target_connection ||= PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
  end

  def self.schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def self.tables
    source_connection.exec(tables_sql).map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def self.column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def self.s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def self.bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def self.redshift_include_tables
    @redshift_include_tables ||= ENV['REDSHIFT_INCLUDE_TABLES'].split(',')
  end

  def self.tables_sql
    sql = "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW') AND table_name !~* '^pg_.*'"
    if ENV['REDSHIFT_INCLUDE_TABLES']
      table_names = "'" + redshift_include_tables.join("', '") + "'"
      sql += " AND table_name IN (#{table_names})"
    end
    sql += " ORDER BY table_name"
    sql
  end

  def self.dry_run?
    ENV['POSTGRES_TO_REDSHIFT_DRY_RUN'] == 'true'
  end
end
