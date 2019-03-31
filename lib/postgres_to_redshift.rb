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
  extend << self

  def update_tables
    incremental? ? incremental_update : full_update
  end

  def incremental?
    ENV['POSTGRES_TO_REDSHIFT_INCREMENTAL'] == 'true' && File.exist?(TIMESTAMP_FILE_NAME)
  end

  def with_retry
    retries_remaining = 2
    begin
      yield
    rescue StandardError => e
      target_connection.exec('ROLLBACK;') rescue nil
      (puts("Import failed due to: #{e.message}") || raise) unless retries_remaining.positive?

      remove_instance_variable(:"@source_connection")
      remove_instance_variable(:"@target_connection")
      sleep 30
      retries_remaining -= 1
      retry
    end
  end

  def with_tracking
    start_time = Time.now.utc
    yield
    File.write(TIMESTAMP_FILE_NAME, start_time.iso8601)
  end

  def in_transaction
    target_connection.exec('BEGIN;')
    yield
    if dry_run?
      target_connection.exec('ROLLBACK;')
    else
      target_connection.exec('COMMIT;')
    end
  end

  def incremental_update
    incremental_from = Time.parse(File.read(TIMESTAMP_FILE_NAME)).utc
    with_tracking do
      with_retry do
        in_transaction do
          tables.each do |table|
            CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema, incremental_from: incremental_from).run
          end
        end
      end
    end
  end

  def full_update
    with_tracking do
      tables.each do |table|
        with_retry do
          CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema).run
        end
      end
    end
  end

  def source_uri
    @source_uri ||= URI.parse(ENV.fetch('POSTGRES_TO_REDSHIFT_SOURCE_URI'))
  end

  def target_uri
    @target_uri ||= URI.parse(ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_URI'))
  end

  def source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
    end

    @source_connection
  end

  def target_connection
    @target_connection ||= PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
  end

  def schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def tables
    source_connection.exec(tables_sql).map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV.fetch('S3_DATABASE_EXPORT_ID'), secret_access_key: ENV.fetch('S3_DATABASE_EXPORT_KEY'))
  end

  def bucket
    @bucket ||= s3.buckets[ENV.fetch('S3_DATABASE_EXPORT_BUCKET')]
  end

  def redshift_include_tables
    @redshift_include_tables ||= ENV['REDSHIFT_INCLUDE_TABLES'].split(',')
  end

  def tables_sql
    sql = "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW') AND table_name !~* '^pg_.*'"
    if ENV['REDSHIFT_INCLUDE_TABLES']
      table_names = "'" + redshift_include_tables.join("', '") + "'"
      sql += " AND table_name IN (#{table_names})"
    end
    sql += ' ORDER BY table_name'
    sql
  end

  def dry_run?
    ENV['POSTGRES_TO_REDSHIFT_DRY_RUN'] == 'true'
  end
end
